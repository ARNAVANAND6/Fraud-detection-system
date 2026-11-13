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

from services.scorer.app.gateway import Gateway, verify_webhook
from services.scorer.app.main import app

KEY = {"X-API-Key": "test-key"}
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
    def __init__(self):
        self.policy = Policy()

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
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", rate)
    r = fakeredis.FakeRedis()
    st = app.state
    st.redis, st.store, st.scorer, st.repo = r, RedisFeatureStore(r), Scorer(tmp_path), FakeRepo()
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


def sent(client_):
    return app.state.gateway.producer.messages


def sign(body: bytes, ts: float | None = None, secret: str = SECRET) -> dict:
    ts = str(int(ts if ts is not None else time.time()))
    sig = hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    return {"X-Vyrox-Timestamp": ts, "X-Vyrox-Signature": f"sha256={sig}", "Content-Type": "application/json"}


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
    assert r.headers["X-Vyrox-Environment"] == "demo"


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
    r = client.post("/v1/imports", headers=KEY, files={"file": ("t.csv", io.BytesIO("\n".join(rows).encode()))})
    assert r.status_code == 202, r.text
    body = r.json()
    assert (body["rows"], body["accepted"], body["duplicates"], body["invalid"]) == (5, 3, 1, 1)
    assert body["first_errors"][0]["row"] == 4
    assert {m[2]["source"] for m in sent(client)} == {"HISTORICAL_IMPORT"}


def test_parquet_import(client):
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame([{**EVENT, "transaction_id": f"PQ_{i}"} for i in range(4)]).to_parquet(buf)
    r = client.post("/v1/imports", headers=KEY, files={"file": ("t.parquet", io.BytesIO(buf.getvalue()))})
    assert r.status_code == 202 and r.json()["accepted"] == 4
    assert client.post("/v1/imports", headers=KEY, files={"file": ("t.txt", io.BytesIO(b"x"))}).status_code == 415


def test_demo_scenario_is_labelled_simulated(client):
    r = client.post("/v1/demo/scenario", json={"scenario": "RAPID_BURST"}, headers=KEY)
    assert r.status_code == 202 and len(r.json()["transaction_ids"]) == 6
    assert {m[2]["source"] for m in sent(client)} == {"SIMULATED_DEMO"}


def test_production_has_no_demo_scenario_and_uses_its_own_topics(prod_client):
    assert prod_client.post("/v1/demo/scenario", json={}, headers=KEY).status_code == 404
    r = prod_client.post("/v1/transactions", json=EVENT, headers=KEY)
    assert r.status_code == 202 and r.headers["X-Vyrox-Environment"] == "production"
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


def test_dry_run_score_contract_and_config(client):
    r = client.post("/v1/score", json=EVENT, headers=KEY)
    assert r.status_code == 200
    body = r.json()
    assert set(body) >= {"risk_score", "risk_level", "decision", "reason_codes", "model_version", "scored_at"}
    assert "BALANCE_DRAIN" in body["reason_codes"] and sent(client) == []
    cfg = client.get("/v1/config", headers=KEY).json()
    cfg["thresholds"] = {"medium": 0.1, "high": 0.2, "critical": 0.3}
    assert client.put("/v1/config", json=cfg, headers=KEY).status_code == 200
    assert client.post("/v1/score", json=EVENT, headers=KEY).json()["risk_level"] == "CRITICAL"
    cfg["thresholds"] = {"medium": 0.5, "high": 0.2, "critical": 0.3}
    assert client.put("/v1/config", json=cfg, headers=KEY).status_code == 422


def test_rate_limit(tmp_path, monkeypatch):
    with make_client(monkeypatch, tmp_path, rate="5") as c:
        codes = [c.post("/v1/score", json=EVENT, headers=KEY).status_code for _ in range(7)]
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)
    assert codes[:5] == [200] * 5 and codes[-1] == 429


def test_health(client):
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").status_code == 200
