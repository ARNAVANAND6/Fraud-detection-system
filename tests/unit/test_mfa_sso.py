"""Two-factor sign-in and single sign-on, through the API with fakes for the database and the provider."""
import base64
import io
import json
import time
import urllib.parse

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from test_api import PASSWORD, make_client

from services.scorer.app import mfa, sso
from services.scorer.app.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    with make_client(monkeypatch, tmp_path) as c:
        yield c
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)
    app.state.sso_provider = None


def login(c, username="admin", password=PASSWORD):
    return c.post("/v1/auth/login", json={"username": username, "password": password})


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def enrol(c, headers):
    secret = c.post("/v1/auth/mfa/setup", headers=headers).json()["secret"]
    r = c.post("/v1/auth/mfa/enable", headers=headers, json={"code": mfa.code_at(secret, mfa.current_step())})
    assert r.status_code == 200, r.text
    return secret, r.json()["recovery_codes"]


# ---- TOTP itself -------------------------------------------------------------------
def test_totp_matches_the_rfc_6238_test_vector():
    secret = base64.b32encode(b"12345678901234567890").decode()
    assert mfa.code_at(secret, 59 // 30) == "287082"            # RFC 6238 appendix B, SHA-1, last 6 digits
    assert mfa.code_at(secret, 1111111109 // 30) == "081804"


def test_a_code_is_accepted_once_and_only_near_its_time():
    secret = mfa.new_secret()
    now = 1_800_000_000
    code = mfa.code_at(secret, mfa.current_step(now))
    step = mfa.match_step(secret, code, None, now)
    assert step is not None
    assert mfa.match_step(secret, code, step, now) is None             # replay refused
    assert mfa.match_step(secret, code, None, now + 31) is not None    # one step of drift allowed
    assert mfa.match_step(secret, code, None, now + 95) is None        # three steps later: refused
    assert mfa.match_step(secret, "12345", None, now) is None


def test_secrets_are_encrypted_at_rest():
    box = mfa.SecretBox("key-one" * 6)
    sealed = box.seal("JBSWY3DPEHPK3PXP")
    assert "JBSWY3DPEHPK3PXP" not in sealed and box.open(sealed) == "JBSWY3DPEHPK3PXP"
    assert mfa.SecretBox("another-key" * 4).open(sealed) is None


# ---- two-factor sign-in through the API ----------------------------------------------
def test_enrol_then_sign_in_needs_the_second_factor(client):
    first = login(client).json()
    secret, recovery = enrol(client, bearer(first["token"]))
    assert len(recovery) == 10 and len(set(recovery)) == 10
    stored = app.state.repo.users["admin"]
    assert secret not in (stored["mfa_secret"] or "") and stored["mfa_enabled"]
    assert not any(code in str(stored["mfa_recovery"]) for code in recovery)    # only digests are kept

    r = login(client).json()
    assert r["mfa_required"] and "token" not in r                # a password alone is not a session any more
    wrong = client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": "000000"})
    assert wrong.status_code == 401
    # The enrolment used the current step, so the next valid code is the next step's.
    code = mfa.code_at(secret, mfa.current_step() + 1)
    ok = client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": code})
    assert ok.status_code == 200 and ok.json()["role"] == "ADMIN"
    assert client.get("/v1/users", headers=bearer(ok.json()["token"])).status_code == 200
    # The same code cannot be used again, and the challenge is gone.
    again = login(client).json()
    assert client.post("/v1/auth/login/mfa", json={"mfa_token": again["mfa_token"], "code": code}).status_code == 401
    assert client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": code}).status_code == 401


def test_recovery_codes_work_once(client):
    _, recovery = enrol(client, bearer(login(client).json()["token"]))
    r = login(client).json()
    ok = client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": recovery[0]})
    assert ok.status_code == 200
    r = login(client).json()
    assert client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": recovery[0]}).status_code == 401
    me = client.get("/v1/auth/mfa", headers=bearer(ok.json()["token"])).json()
    assert me == {"enabled": True, "required": False, "recovery_codes_left": 9}


def test_wrong_codes_use_up_the_challenge(client):
    enrol(client, bearer(login(client).json()["token"]))
    r = login(client).json()
    for _ in range(5):
        client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": "000000"})
    secret_box = mfa.SecretBox(app.state.settings.secret_key)
    secret = secret_box.open(app.state.repo.users["admin"]["mfa_secret"])
    good = mfa.code_at(secret, mfa.current_step() + 1)
    assert client.post("/v1/auth/login/mfa", json={"mfa_token": r["mfa_token"], "code": good}).status_code == 401
    assert any(a["action"] == "MFA_FAILED" for a in app.state.repo.audit_rows)


def test_required_role_must_enrol_before_doing_anything(client, monkeypatch):
    app.state.settings = (app.state.settings.__class__(
        **{**app.state.settings.__dict__, "mfa_required_roles": ("ADMIN",)}))
    r = login(client).json()
    assert r["mfa_enrolment_required"]
    h = bearer(r["token"])
    assert client.get("/v1/users", headers=h).status_code == 403
    assert client.get("/v1/auth/me", headers=h).json()["mfa_enrolment_required"]
    enrol(client, h)
    assert client.get("/v1/users", headers=h).status_code == 200   # the same session is unlocked by enrolling
    analyst = login(client, "asha").json()
    assert "mfa_enrolment_required" not in analyst                   # other roles are not affected


def test_admin_can_reset_a_lost_authenticator(client):
    asha = bearer(login(client, "asha").json()["token"])
    enrol(client, asha)
    admin = bearer(login(client).json()["token"])
    r = client.patch("/v1/users/asha", headers=admin, json={"reset_mfa": True})
    assert r.status_code == 200 and r.json()["mfa_enabled"] is False
    assert client.get("/v1/alerts", headers=asha).status_code == 401    # her sessions ended
    assert "token" in login(client, "asha").json()                       # password alone works again


# ---- single sign-on ------------------------------------------------------------------
ISSUER = "https://idp.example.test"


class FakeProvider:
    """Serves discovery, keys and a token endpoint like a real OpenID provider."""

    def __init__(self):
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.claims_override, self.nonce, self.alg = {}, None, "RS256"

    def __call__(self, req, timeout=10):
        url = req.full_url
        if url.endswith("/.well-known/openid-configuration"):
            body = {"issuer": ISSUER, "authorization_endpoint": f"{ISSUER}/authorize",
                    "token_endpoint": f"{ISSUER}/token", "jwks_uri": f"{ISSUER}/jwks"}
        elif url.endswith("/jwks"):
            jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(self.key.public_key()))
            body = {"keys": [{**jwk, "kid": "k1", "use": "sig", "alg": "RS256"}]}
        elif url.endswith("/token"):
            form = urllib.parse.parse_qs(req.data.decode())
            assert form["code"] == ["good-code"] and form["code_verifier"][0]
            now = int(time.time())
            claims = {"iss": ISSUER, "aud": "vyrox", "sub": "user-123", "iat": now, "exp": now + 300,
                      "nonce": self.nonce, "email": "Priya@Bank.example", "email_verified": True,
                      **self.claims_override}
            key = self.key if self.alg == "RS256" else "a-shared-secret-that-is-at-least-32-bytes"
            body = {"id_token": jwt.encode(claims, key, algorithm=self.alg, headers={"kid": "k1"})}
        else:
            raise AssertionError(url)
        return io.BytesIO(json.dumps(body).encode())


@pytest.fixture
def sso_client(client, monkeypatch):
    s = app.state.settings
    app.state.settings = (s.__class__(**{
        **s.__dict__, "oidc_issuer": ISSUER, "oidc_client_id": "vyrox", "oidc_client_secret": "shh",
        "oidc_redirect_url": "https://risk.example/v1/auth/sso/callback", "dashboard_url": "https://risk.example",
        "oidc_provider_name": "Bank SSO"}))
    fake = FakeProvider()
    app.state.sso_provider = sso.Provider(ISSUER, opener=fake)
    return client, fake


def run_sso(c, fake, code="good-code"):
    start = c.get("/v1/auth/sso/start", follow_redirects=False)
    assert start.status_code == 302
    q = urllib.parse.parse_qs(urllib.parse.urlparse(start.headers["location"]).query)
    assert q["code_challenge_method"] == ["S256"] and q["client_id"] == ["vyrox"]
    fake.nonce = q["nonce"][0]
    back = c.get("/v1/auth/sso/callback", params={"code": code, "state": q["state"][0]}, follow_redirects=False)
    assert back.status_code == 302 and back.headers["location"].startswith("https://risk.example/#")
    return urllib.parse.parse_qs(urllib.parse.urlparse(back.headers["location"]).fragment), q["state"][0]


def test_sso_config_is_public_and_off_by_default(client):
    assert client.get("/v1/auth/sso").json() == {"enabled": False, "provider_name": None}
    assert client.get("/v1/auth/sso/start", follow_redirects=False).status_code == 404


def test_sso_signs_in_a_person_an_admin_added_by_email(sso_client):
    c, fake = sso_client
    assert c.get("/v1/auth/sso").json() == {"enabled": True, "provider_name": "Bank SSO"}
    app.state.repo.create_user("priya", "x", "ANALYST", "priya@bank.example")
    frag, state = run_sso(c, fake)
    token = frag["sso_token"][0]
    assert c.get("/v1/auth/me", headers=bearer(token)).json() == {
        "username": "priya", "role": "ANALYST", "org_id": "demo", "org_name": "Demo Bank"}
    assert app.state.repo.users["priya"]["sso_subject"] == f"{ISSUER}|user-123"
    # The state is single use: replaying the callback fails.
    replay = c.get("/v1/auth/sso/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert "sso_error" in urllib.parse.parse_qs(urllib.parse.urlparse(replay.headers["location"]).fragment)


def test_sso_refuses_unknown_people_unless_their_domain_is_allowed(sso_client, monkeypatch):
    c, fake = sso_client
    frag, _ = run_sso(c, fake)
    assert "no Foresentry account" in frag["sso_error"][0]
    s = app.state.settings
    app.state.settings = (s.__class__(**{**s.__dict__, "oidc_allowed_domains": ("bank.example",)}))
    frag, _ = run_sso(c, fake)
    me = c.get("/v1/auth/me", headers=bearer(frag["sso_token"][0])).json()
    assert me == {"username": "priya", "role": "VIEWER", "org_id": "demo", "org_name": "Demo Bank"}
    assert login(c, "priya", "anything-at-all").status_code == 401      # SSO-made accounts have no password


@pytest.mark.parametrize("override, message", [
    ({"email_verified": False}, "verified e-mail"),
    ({"aud": "someone-else"}, "not valid"),
    ({"iss": "https://evil.test"}, "not valid"),
    ({"exp": int(time.time()) - 600}, "not valid"),
    ({"nonce": "wrong"}, "nonce"),
])
def test_sso_rejects_bad_id_tokens(sso_client, override, message):
    c, fake = sso_client
    app.state.repo.create_user("priya", "x", "ANALYST", "priya@bank.example")
    fake.claims_override = override
    frag, _ = run_sso(c, fake)
    assert "sso_token" not in frag and message in frag["sso_error"][0]


def test_sso_rejects_shared_secret_tokens(sso_client):
    c, fake = sso_client
    app.state.repo.create_user("priya", "x", "ANALYST", "priya@bank.example")
    fake.alg = "HS256"
    frag, _ = run_sso(c, fake)
    assert "sso_token" not in frag and "shared-secret" in frag["sso_error"][0]
