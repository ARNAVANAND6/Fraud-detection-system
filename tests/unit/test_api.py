"""API tests with fakes for Kafka and PostgreSQL. The real pipeline is covered
by tests/acceptance, which runs against the live stack."""
import hashlib
import hmac
import io
import json
import time
from collections import deque

import fakeredis
import pytest
from fastapi.testclient import TestClient
from fraudlib.config import get_settings
from fraudlib.features import RedisFeatureStore
from fraudlib.policy import Policy
from fraudlib.scoring import Scorer

from services.scorer.app.auth import hash_password, verify_password
from services.scorer.app.gateway import Gateway, verify_webhook
from services.scorer.app.main import app

KEY = {"X-API-Key": "test-key"}
PASSWORD = "correct-horse-battery"
PASSWORD_HASH = hash_password(PASSWORD)  # hashed once: scrypt is slow on purpose
SECRET = "test-webhook-secret"
EVENT = {"transaction_id": "TXN_1", "event_time": "2026-01-01T00:00:00+00:00", "account_id": "C1",
         "counterparty_id": "C2", "txn_type": "TRANSFER", "amount": 900.0, "old_balance_orig": 900.0,
         "old_balance_dest": 0.0}


class FakeProducer:
    def __init__(self):
        self.messages, self.fail = [], False

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if self.fail:
            class Msg:
                def key(self_inner):
                    return key.encode() if isinstance(key, str) else key
            on_delivery("broker down", Msg())
            return
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


class FakeRepo:
    """In-memory stand-in for the PostgreSQL repository, with the same rules for users and sessions."""

    def __init__(self):
        self.policy = Policy()
        self.users, self.sessions, self.audit_rows, self.alert_updates = {}, {}, [], []
        self.challenges, self.sso_states = {}, {}
        self.api_keys = {}
        self.orgs = {"demo": {"org_id": "demo", "name": "Demo Bank", "region": "IN", "status": "ACTIVE"}}

    def get_user(self, username):
        return self.users.get(username)

    @staticmethod
    def _public(u):
        return {"username": u["username"], "role": u["role"], "org_id": u["org_id"],
                "account_ids": u["account_ids"], "active": u["active"], "email": u["email"],
                "mfa_enabled": u["mfa_enabled"], "sso": u["sso_subject"] is not None}

    def list_users(self, org=None):
        return [self._public(u) for u in self.users.values() if org is None or u["org_id"] == org]

    def create_org(self, org_id, name, region):
        if org_id in self.orgs:
            return None
        self.orgs[org_id] = {"org_id": org_id, "name": name, "region": region, "status": "ACTIVE"}
        return self.orgs[org_id]

    def update_org(self, org_id, name, region, status):
        o = self.orgs.get(org_id)
        if o is None:
            return None
        o.update({k: v for k, v in (("name", name), ("region", region), ("status", status)) if v})
        if status == "SUSPENDED":
            for t, (u, _) in list(self.sessions.items()):
                if self.users[u]["org_id"] == org_id:
                    del self.sessions[t]
        return o

    def get_org(self, org_id):
        return self.orgs.get(org_id)

    def org_active(self, org_id):
        return self.orgs.get(org_id, {}).get("status") == "ACTIVE"

    def find_api_key(self, key_sha256):
        k = self.api_keys.get(key_sha256)
        if k is None or k["revoked_at"]:
            return None
        return {"org_id": k["org_id"], "prefix": k["prefix"], "org_status": self.orgs[k["org_id"]]["status"]}

    def create_api_key(self, key_sha256, org_id, name, prefix, created_by):
        self.api_keys[key_sha256] = {"key_id": key_sha256[:12], "org_id": org_id, "name": name, "prefix": prefix,
                                     "created_by": created_by, "revoked_at": None}
        return {k: v for k, v in self.api_keys[key_sha256].items() if k != "revoked_at"}

    def list_api_keys(self, org):
        return [k for k in self.api_keys.values() if org is None or k["org_id"] == org]

    def revoke_api_key(self, key_id, org):
        for k in self.api_keys.values():
            if k["key_id"] == key_id and not k["revoked_at"] and (org is None or k["org_id"] == org):
                k["revoked_at"] = "now"
                return True
        return False

    def create_user(self, username, password_hash, role, email=None, org_id="demo", account_ids=()):
        if username in self.users or (email and any(u["email"] == email for u in self.users.values())):
            return False
        self.users[username] = {"username": username, "password_hash": password_hash, "role": role, "active": True,
                                "org_id": None if role == "SUPERADMIN" else org_id, "account_ids": list(account_ids),
                                "email": email, "sso_subject": None, "mfa_enabled": False, "mfa_secret": None,
                                "mfa_pending": None, "mfa_last_step": None, "mfa_recovery": []}
        return True

    def update_user(self, username, role, active, password_hash, reset_mfa=False, email=None, org=None,
                    account_ids=None):
        u = self.users.get(username)
        if u is None or (org and u["org_id"] != org):
            return None
        loses_admin = u["role"] == "ADMIN" and u["active"] and (
            (role is not None and role != "ADMIN") or active is False)
        others = [o for n, o in self.users.items() if n != username and o["role"] == "ADMIN" and o["active"]]
        if loses_admin and not others:
            raise ValueError("this is the last active administrator")
        changed_role = role is not None and role != u["role"]
        u.update({k: v for k, v in (("role", role), ("active", active), ("password_hash", password_hash),
                                    ("email", email), ("account_ids", account_ids)) if v is not None})
        if reset_mfa:
            u.update(mfa_enabled=False, mfa_secret=None, mfa_recovery=[])
        if active is False or password_hash is not None or changed_role or reset_mfa:
            self.sessions = {t: v for t, v in self.sessions.items() if v[0] != username}
        return self._public(u)

    def create_session(self, token_sha256, username, expires_at, limited=False, method="password"):
        self.sessions[token_sha256] = (username, limited)

    def get_session_user(self, token_sha256):
        name, limited = self.sessions.get(token_sha256, (None, False))
        u = self.users.get(name)
        if not (u and u["active"]):
            return None
        return {"username": u["username"], "role": u["role"], "limited": limited, "org_id": u["org_id"],
                "account_ids": u["account_ids"],
                "org_status": self.orgs[u["org_id"]]["status"] if u["org_id"] else None}

    def delete_session(self, token_sha256):
        self.sessions.pop(token_sha256, None)

    def create_challenge(self, token_sha256, username, expires_at):
        self.challenges[token_sha256] = {"username": username, "attempts": 0}

    def use_challenge(self, token_sha256, max_attempts):
        c = self.challenges.get(token_sha256)
        if c is None or c["attempts"] >= max_attempts:
            return None
        c["attempts"] += 1
        return dict(c)

    def delete_challenge(self, token_sha256):
        self.challenges.pop(token_sha256, None)

    def set_mfa_last_step(self, username, step):
        u = self.users[username]
        if u["mfa_last_step"] is not None and u["mfa_last_step"] >= step:
            return False
        u["mfa_last_step"] = step
        return True

    def use_recovery_code(self, username, digest):
        u = self.users[username]
        if u["mfa_enabled"] and digest in u["mfa_recovery"]:
            u["mfa_recovery"].remove(digest)
            return True
        return False

    def set_mfa_pending(self, username, sealed):
        self.users[username]["mfa_pending"] = sealed

    def enable_mfa(self, username, digests, step):
        u = self.users[username]
        u.update(mfa_secret=u["mfa_pending"], mfa_pending=None, mfa_enabled=True, mfa_recovery=list(digests),
                 mfa_last_step=step)
        self.sessions = {t: (n, False if n == username else lim) for t, (n, lim) in self.sessions.items()}

    def disable_mfa(self, username):
        self.users[username].update(mfa_enabled=False, mfa_secret=None, mfa_pending=None, mfa_recovery=[])

    def find_user_for_sso(self, subject, email):
        for u in self.users.values():
            if u["sso_subject"] == subject:
                return u
        for u in self.users.values():
            if email and u["email"] == email:
                return u
        return None

    def link_sso_subject(self, username, subject):
        if self.users[username]["sso_subject"] is None:
            self.users[username]["sso_subject"] = subject

    def save_sso_state(self, state_sha256, nonce, verifier, expires_at):
        self.sso_states[state_sha256] = {"nonce": nonce, "code_verifier": verifier}

    def pop_sso_state(self, state_sha256):
        return self.sso_states.pop(state_sha256, None)

    def audit(self, actor, role, action, target, detail, ip, org_id=None):
        self.audit_rows.append({"actor": actor, "role": role, "action": action, "target": target, "detail": detail,
                                "org_id": org_id})

    def list_audit(self, limit, org=None):
        return [a for a in self.audit_rows[::-1] if org is None or a["org_id"] == org][:limit]

    def update_alert(self, alert_id, to_status, assign_to, actor, notes, org=None):
        self.alert_updates.append((alert_id, to_status, actor))
        return {"alert_id": alert_id, "status": to_status, "account_id": "C1", "priority": 1, "trigger_types": [],
                "reason_codes": [], "max_risk_score": 0.9, "event_count": 1, "assigned_to": assign_to,
                "escalation_level": 0, "sla_due_at": None, "created_at": None, "updated_at": None}

    def ping(self):
        return True

    def get_policy(self):
        return self.policy

    def set_policy(self, policy, actor):
        self.policy = policy
        return policy

    def close(self):
        pass


def make_client(monkeypatch, tmp_path, environment="demo", rate="1000"):
    monkeypatch.setenv("API_KEYS", "test-key")
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("ADMIN_PASSWORD", PASSWORD)
    monkeypatch.setenv("SUPERADMIN_PASSWORD", PASSWORD + "-super")
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", rate)
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-that-is-long-enough")
    r = fakeredis.FakeRedis()
    st = app.state
    st.redis, st.store, st.scorer, st.repo = r, RedisFeatureStore(r), Scorer(tmp_path), FakeRepo()
    for name, role in (("admin", "ADMIN"), ("asha", "ANALYST"), ("vik", "VIEWER"), ("root", "SUPERADMIN")):
        st.repo.create_user(name, PASSWORD_HASH, role)
    gw = Gateway.__new__(Gateway)
    gw.s, gw.redis, gw.producer, gw.ingest_ms = get_settings(), r, FakeProducer(), deque(maxlen=100)
    st.gateway = gw
    return TestClient(app)


@pytest.fixture
def client(tmp_path, monkeypatch):
    with make_client(monkeypatch, tmp_path) as c:
        yield c
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)


@pytest.fixture
def prod_client(tmp_path, monkeypatch):
    with make_client(monkeypatch, tmp_path, environment="production") as c:
        yield c
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)


def as_user(c, username="admin", password=PASSWORD) -> dict:
    r = c.post("/v1/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def sent(client_):
    return app.state.gateway.producer.messages


def sign(body: bytes, ts: float | None = None, secret: str = SECRET) -> dict:
    ts = str(int(ts if ts is not None else time.time()))
    sig = hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    return {"X-Foresentry-Timestamp": ts, "X-Foresentry-Signature": f"sha256={sig}", "Content-Type": "application/json"}


def test_requires_api_key(client):
    assert client.post("/v1/transactions", json=EVENT).status_code == 401
    assert client.post("/v1/transactions", json=EVENT, headers={"X-API-Key": "wrong"}).status_code == 401
    assert sent(client) == []


def test_rest_ingest_publishes_once_and_stamps_source(client):
    r = client.post("/v1/transactions", json=EVENT, headers=KEY)
    assert r.status_code == 202 and r.json()["status"] == "accepted" and r.json()["source"] == "LIVE_API"
    again = client.post("/v1/transactions", json=EVENT, headers=KEY)
    assert again.status_code == 202 and again.json()["status"] == "duplicate"
    assert len(sent(client)) == 1
    topic, key, value = sent(client)[0]
    assert topic == "demo.payments.transactions.v1" and key == "C1"
    assert value["source"] == "LIVE_API" and value["ingested_at"]
    assert r.headers["X-Foresentry-Environment"] == "demo"


def test_caller_cannot_choose_the_source_label(client):
    r = client.post("/v1/transactions", json={**EVENT, "source": "HISTORICAL_IMPORT"}, headers=KEY)
    assert r.status_code == 202
    assert sent(client)[0][2]["source"] == "LIVE_API"


def test_validation_error_is_422_and_nothing_is_published(client):
    r = client.post("/v1/transactions", json={**EVENT, "amount": -5}, headers=KEY)
    assert r.status_code == 422 and sent(client) == []


def test_failed_publish_returns_503_and_can_be_retried(client):
    app.state.gateway.producer.fail = True
    assert client.post("/v1/transactions", json=EVENT, headers=KEY).status_code == 503
    app.state.gateway.producer.fail = False
    r = client.post("/v1/transactions", json=EVENT, headers=KEY)
    assert r.json()["status"] == "accepted" and len(sent(client)) == 1


def test_webhook_signature(client):
    body = json.dumps({"event_id": "evt_1", "type": "payment.created", "data": EVENT}).encode()
    assert client.post("/v1/webhooks/generic", content=body).status_code == 401
    assert client.post("/v1/webhooks/generic", content=body, headers=sign(body, secret="other")).status_code == 401
    assert client.post("/v1/webhooks/generic", content=body,
                       headers=sign(body, ts=time.time() - 3600)).status_code == 401
    tampered = body.replace(b"900.0", b"1.0")
    assert client.post("/v1/webhooks/generic", content=tampered, headers=sign(body)).status_code == 401
    assert sent(client) == []
    ok = client.post("/v1/webhooks/generic", content=body, headers=sign(body))
    assert ok.status_code == 202 and ok.json()["source"] == "LIVE_WEBHOOK" and ok.json()["status"] == "accepted"
    redelivery = client.post("/v1/webhooks/generic", content=body, headers=sign(body))
    assert redelivery.json()["status"] == "duplicate" and len(sent(client)) == 1
    assert sent(client)[0][2]["source"] == "LIVE_WEBHOOK"


def test_verify_webhook_unit():
    assert verify_webhook("", "1", "x", b"") == "webhook secret is not configured"
    assert verify_webhook("s", "abc", "x", b"") == "bad timestamp"


def test_csv_import_labels_rows_and_reports_bad_ones(client):
    header = "transaction_id,event_time,account_id,counterparty_id,txn_type,amount,old_balance_orig,old_balance_dest"
    rows = [header] + [f"IMP_{i},2026-01-01T00:00:0{i}+00:00,C{i},P1,PAYMENT,10.5,100,0" for i in range(3)]
    rows.append("IMP_BAD,2026-01-01T00:00:09+00:00,C9,P1,PAYMENT,-4,100,0")
    rows.append("IMP_0,2026-01-01T00:00:00+00:00,C0,P1,PAYMENT,10.5,100,0")  # duplicate inside the file
    r = client.post("/v1/imports", headers=as_user(client),
                    files={"file": ("t.csv", io.BytesIO("\n".join(rows).encode()))})
    assert r.status_code == 202, r.text
    body = r.json()
    assert (body["rows"], body["accepted"], body["duplicates"], body["invalid"]) == (5, 3, 1, 1)
    assert body["first_errors"][0]["row"] == 4
    assert {m[2]["source"] for m in sent(client)} == {"HISTORICAL_IMPORT"}


def test_parquet_import(client):
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame([{**EVENT, "transaction_id": f"PQ_{i}"} for i in range(4)]).to_parquet(buf)
    r = client.post("/v1/imports", headers=as_user(client), files={"file": ("t.parquet", io.BytesIO(buf.getvalue()))})
    assert r.status_code == 202 and r.json()["accepted"] == 4
    r = client.post("/v1/imports", headers=as_user(client), files={"file": ("t.txt", io.BytesIO(b"x"))})
    assert r.status_code == 415


def test_demo_scenario_is_labelled_simulated(client):
    r = client.post("/v1/demo/scenario", json={"scenario": "RAPID_BURST"}, headers=as_user(client))
    assert r.status_code == 202 and len(r.json()["transaction_ids"]) == 6
    assert {m[2]["source"] for m in sent(client)} == {"SIMULATED_DEMO"}


def test_production_has_no_demo_scenario_and_uses_its_own_topics(prod_client):
    assert prod_client.post("/v1/demo/scenario", json={}, headers=as_user(prod_client)).status_code == 404
    r = prod_client.post("/v1/transactions", json=EVENT, headers=KEY)
    assert r.status_code == 202 and r.headers["X-Foresentry-Environment"] == "production"
    assert sent(prod_client)[0][0] == "production.payments.transactions.v1"
    from fraudlib.schemas import Source, TransactionEvent
    with pytest.raises(PermissionError):
        app.state.gateway.ingest([TransactionEvent(**{**EVENT, "transaction_id": "X"})], Source.SIMULATED_DEMO)


def test_production_refuses_demo_credentials(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("API_KEYS", "dev-key")
    monkeypatch.setenv("WEBHOOK_SECRET", "demo-webhook-secret")
    with pytest.raises(RuntimeError):
        get_settings().assert_safe_for_production()
    monkeypatch.setenv("API_KEYS", "a-real-key")
    monkeypatch.setenv("WEBHOOK_SECRET", "a-real-secret")
    with pytest.raises(RuntimeError, match="ADMIN_PASSWORD"):
        get_settings().assert_safe_for_production()  # the demo administrator password alone is enough to refuse
    monkeypatch.setenv("ADMIN_PASSWORD", "a-real-admin-password")
    with pytest.raises(RuntimeError, match="SUPERADMIN_PASSWORD"):
        get_settings().assert_safe_for_production()
    monkeypatch.setenv("SUPERADMIN_PASSWORD", "a-real-superadmin-password")
    with pytest.raises(RuntimeError, match="SECRET_KEY"):
        get_settings().assert_safe_for_production()  # the demo key would let anyone decrypt 2FA secrets
    monkeypatch.setenv("SECRET_KEY", "x" * 40)
    get_settings().assert_safe_for_production()


def test_dry_run_score_contract_and_config(client):
    r = client.post("/v1/score", json=EVENT, headers=KEY)
    assert r.status_code == 200
    body = r.json()
    assert set(body) >= {"risk_score", "risk_level", "decision", "reason_codes", "model_version", "scored_at"}
    assert "BALANCE_DRAIN" in body["reason_codes"] and sent(client) == []
    cfg, admin = client.get("/v1/config", headers=KEY).json(), as_user(client, "root")
    cfg["thresholds"] = {"medium": 0.1, "high": 0.2, "critical": 0.3}
    assert client.put("/v1/config", json=cfg, headers=admin).status_code == 200
    assert client.post("/v1/score", json=EVENT, headers=KEY).json()["risk_level"] == "CRITICAL"
    cfg["thresholds"] = {"medium": 0.5, "high": 0.2, "critical": 0.3}
    assert client.put("/v1/config", json=cfg, headers=admin).status_code == 422


def test_rate_limit(tmp_path, monkeypatch):
    with make_client(monkeypatch, tmp_path, rate="5") as c:
        codes = [c.post("/v1/score", json=EVENT, headers=KEY).status_code for _ in range(7)]
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)
    assert codes[:5] == [200] * 5 and codes[-1] == 429


def test_health(client):
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").status_code == 200


# ---- sign-in, roles, audit ---------------------------------------------------------
def test_password_hashing():
    assert PASSWORD_HASH.startswith("scrypt$") and PASSWORD not in PASSWORD_HASH
    assert verify_password(PASSWORD, PASSWORD_HASH)
    assert not verify_password(PASSWORD + "x", PASSWORD_HASH)
    assert not verify_password(PASSWORD, "not-a-hash")
    assert hash_password(PASSWORD) != PASSWORD_HASH  # salted


def test_sign_in_and_out(client):
    assert client.get("/v1/auth/me").status_code == 401
    assert client.post("/v1/auth/login", json={"username": "asha", "password": "wrong-password"}).status_code == 401
    assert client.post("/v1/auth/login", json={"username": "nobody", "password": PASSWORD}).status_code == 401
    token = as_user(client, "asha")
    assert client.get("/v1/auth/me", headers=token).json() == {
        "username": "asha", "role": "ANALYST", "org_id": "demo", "org_name": "Demo Bank"}
    assert client.get("/v1/auth/me", headers=KEY).status_code == 403  # an API key is not a person
    assert client.post("/v1/auth/logout", headers=token).status_code == 204
    assert client.get("/v1/auth/me", headers=token).status_code == 401
    actions = [(a["actor"], a["action"]) for a in app.state.repo.audit_rows]
    assert ("asha", "LOGIN_FAILED") in actions and ("asha", "LOGIN") in actions and ("asha", "LOGOUT") in actions
    assert PASSWORD not in json.dumps(app.state.repo.audit_rows)


def test_repeated_failed_sign_ins_lock_out(client):
    codes = [client.post("/v1/auth/login", json={"username": "admin", "password": "bad-password-x"}).status_code
             for _ in range(6)]
    assert codes == [401] * 5 + [429]
    # Even the right password is refused while locked.
    assert client.post("/v1/auth/login", json={"username": "admin", "password": PASSWORD}).status_code == 429


def test_roles_limit_what_each_caller_can_do(client):
    viewer, analyst, admin = as_user(client, "vik"), as_user(client, "asha"), as_user(client)
    cfg = client.get("/v1/config", headers=viewer).json()  # everyone signed in can read
    update = {"status": "IN_REVIEW"}
    # Working an alert needs an analyst or admin, and never an API key.
    assert client.patch("/v1/alerts/7", json=update, headers=viewer).status_code == 403
    assert client.patch("/v1/alerts/7", json=update, headers=KEY).status_code == 403
    assert client.patch("/v1/alerts/7", json=update, headers=analyst).status_code == 200
    # Users and the audit log are admin only. Settings apply to every organisation, so only the superadmin.
    for who in (viewer, analyst, KEY):
        assert client.put("/v1/config", json=cfg, headers=who).status_code == 403
        assert client.get("/v1/users", headers=who).status_code == 403
        assert client.get("/v1/audit", headers=who).status_code == 403
    assert client.put("/v1/config", json=cfg, headers=admin).status_code == 403
    assert client.put("/v1/config", json=cfg, headers=as_user(client, "root")).status_code == 200
    # People below admin cannot push transactions; machines with a key can.
    assert client.post("/v1/transactions", json=EVENT, headers=analyst).status_code == 403
    assert client.post("/v1/transactions", json=EVENT, headers=KEY).status_code == 202
    denied = [a for a in app.state.repo.audit_rows if a["action"] == "DENIED"]
    assert {"actor": "vik", "target": "PATCH /v1/alerts/7"}.items() <= denied[0].items()


def test_alert_changes_are_attributed_to_the_signed_in_person(client):
    analyst = as_user(client, "asha")
    r = client.patch("/v1/alerts/7", json={"status": "CONFIRMED_FRAUD", "actor": "someone-else"}, headers=analyst)
    assert r.status_code == 200
    assert app.state.repo.alert_updates == [(7, "CONFIRMED_FRAUD", "asha")]  # the body cannot choose the actor
    last = app.state.repo.audit_rows[-1]
    assert (last["actor"], last["action"], last["target"]) == ("asha", "ALERT_UPDATE", "7")


def test_user_management(client):
    admin = as_user(client)
    new = {"username": "Meera", "password": "long-enough-pass", "role": "ANALYST"}
    assert client.post("/v1/users", json={**new, "password": "short"}, headers=admin).status_code == 422
    assert client.post("/v1/users", json={**new, "username": "x"}, headers=admin).status_code == 422
    assert client.post("/v1/users", json=new, headers=admin).status_code == 201
    assert client.post("/v1/users", json=new, headers=admin).status_code == 409
    listed = client.get("/v1/users", headers=admin).json()
    assert {u["username"] for u in listed} == {"admin", "asha", "vik", "meera"}
    assert all("password_hash" not in u for u in listed)
    meera = as_user(client, "meera", "long-enough-pass")
    # Deactivating signs the person out at once.
    assert client.patch("/v1/users/meera", json={"active": False}, headers=admin).status_code == 200
    assert client.get("/v1/auth/me", headers=meera).status_code == 401
    assert client.post("/v1/auth/login", json={"username": "meera", "password": "long-enough-pass"}).status_code == 401
    # The last administrator cannot be demoted or deactivated.
    assert client.patch("/v1/users/admin", json={"role": "VIEWER"}, headers=admin).status_code == 409
    assert client.patch("/v1/users/admin", json={"active": False}, headers=admin).status_code == 409
    assert client.patch("/v1/users/ghost", json={"active": False}, headers=admin).status_code == 404
    audit = client.get("/v1/audit", headers=admin).json()
    assert {"USER_CREATE", "USER_CHANGE"} <= {a["action"] for a in audit}
    assert "long-enough-pass" not in json.dumps(audit)


def test_drift_endpoint_reports_missing_reference_then_real_comparison(client):
    from fraudlib import drift
    from fraudlib.features import FEATURE_NAMES

    assert client.get("/v1/model/drift").status_code == 401
    app.state.drift_cache = None
    assert client.get("/v1/model/drift", headers=KEY).json()["status"] == "NO_REFERENCE"

    import numpy as np
    rng = np.random.default_rng(1)
    train = rng.random((5000, len(FEATURE_NAMES)))
    app.state.scorer.reference = drift.build_reference(train, FEATURE_NAMES)
    live = [dict(zip(FEATURE_NAMES, r, strict=True)) for r in (rng.random((1000, len(FEATURE_NAMES))) * 3).tolist()]
    app.state.repo.recent_features = lambda: (live, 24.0)
    app.state.drift_cache = None
    body = client.get("/v1/model/drift", headers=KEY).json()
    assert body["status"] == "DRIFTED" and body["rows"] == 1000 and len(body["features"]) == len(FEATURE_NAMES)
    app.state.drift_cache = None
