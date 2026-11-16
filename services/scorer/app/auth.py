"""Sign-in, roles and the audit trail.

Two kinds of caller:

- People sign in with a username and password and get a session token
  (`Authorization: Bearer ...`). Each person has one role:
  VIEWER (read), ANALYST (read + work alerts), ADMIN (everything).
- Machines send `X-API-Key`. A key can ingest and read, and nothing else:
  it cannot work alerts, change settings or manage users, because those
  actions must be traceable to a person.

Passwords are stored as scrypt hashes. Session tokens are random, stored
only as SHA-256 digests, and expire. Every sign-in attempt and every change
made through the API is written to the audit log with who did it.

Two-factor sign-in (authenticator app codes) is in mfa.py and single
sign-on (OpenID Connect) in sso.py. Both end in the same kind of session.
TLS comes from the proxy in front of the API (infra/proxy).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fastapi import Header, HTTPException, Request
from redis.exceptions import RedisError

ROLES = ("VIEWER", "ANALYST", "ADMIN")
SERVICE = "SERVICE"  # an API key, not a person
MIN_PASSWORD_LENGTH = 10
SESSION_HOURS = 8
CHALLENGE_MINUTES = 5
CHALLENGE_ATTEMPTS = 5
_SCRYPT = {"n": 2**14, "r": 8, "p": 1}

AUTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    username       TEXT PRIMARY KEY CHECK (username ~ '^[a-z0-9._-]{3,32}$'),
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL CHECK (role IN ('VIEWER', 'ANALYST', 'ADMIN')),
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sessions (
    token_sha256  TEXT PRIMARY KEY,
    username      TEXT NOT NULL REFERENCES users (username) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_username_idx ON sessions (username);
-- Append-only: the API never updates or deletes rows here.
CREATE TABLE IF NOT EXISTS audit_log (
    id      BIGSERIAL PRIMARY KEY,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor   TEXT NOT NULL,
    role    TEXT NOT NULL,
    action  TEXT NOT NULL,
    target  TEXT,
    detail  JSONB NOT NULL DEFAULT '{}',
    ip      TEXT
);
CREATE INDEX IF NOT EXISTS audit_log_at_idx ON audit_log (at DESC);
-- Two-factor sign-in and single sign-on (added later; ALTER keeps older databases working).
ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_subject TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_secret TEXT;          -- encrypted, see mfa.SecretBox
ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_pending TEXT;         -- encrypted, set up but not confirmed
ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_last_step BIGINT;     -- no code is accepted twice
ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_recovery TEXT[] NOT NULL DEFAULT '{}';  -- SHA-256 digests
CREATE UNIQUE INDEX IF NOT EXISTS users_email_idx ON users (lower(email)) WHERE email IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS users_sso_subject_idx ON users (sso_subject) WHERE sso_subject IS NOT NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS limited BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS method TEXT NOT NULL DEFAULT 'password';
-- Password accepted, second factor still owed. Short-lived, single use, attempt-limited.
CREATE TABLE IF NOT EXISTS login_challenges (
    token_sha256  TEXT PRIMARY KEY,
    username      TEXT NOT NULL REFERENCES users (username) ON DELETE CASCADE,
    expires_at    TIMESTAMPTZ NOT NULL,
    attempts      INT NOT NULL DEFAULT 0
);
-- Single sign-on round trips in flight: state, nonce and PKCE verifier, single use.
CREATE TABLE IF NOT EXISTS sso_states (
    state_sha256   TEXT PRIMARY KEY,
    nonce          TEXT NOT NULL,
    code_verifier  TEXT NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL
);
"""


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, dklen=32, **_SCRYPT)
    return "scrypt${n}${r}${p}${salt}${digest}".format(
        **_SCRYPT, salt=base64.b64encode(salt).decode(), digest=base64.b64encode(digest).decode())


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = stored.split("$")
        if scheme != "scrypt":
            return False
        want = base64.b64decode(digest)
        got = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), dklen=len(want),
                             n=int(n), r=int(r), p=int(p))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(got, want)


# Verified against when the username does not exist, so a wrong username
# costs the same time as a wrong password.
_DUMMY_HASH = hash_password(secrets.token_urlsafe(16))


def check_password_rules(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise HTTPException(status_code=422, detail=f"password must be at least {MIN_PASSWORD_LENGTH} characters")


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class LoginThrottle:
    """Locks a username + address pair after repeated failures.

    Kept in Redis so every API replica sees the same count and a restart does not
    reset it. If Redis is unreachable it falls back to this process's memory.
    """

    def __init__(self, redis=None, max_failures: int = 5, window_s: int = 900):
        self.redis, self.max_failures, self.window_s = redis, max_failures, window_s
        self.failures: dict[str, deque] = defaultdict(deque)

    @staticmethod
    def _key(key: str) -> str:
        return "lt:" + hashlib.sha256(key.encode()).hexdigest()[:32]

    def _prune(self, key: str) -> deque:
        q, now = self.failures[key], time.time()
        while q and now - q[0] > self.window_s:
            q.popleft()
        return q

    def locked(self, key: str) -> bool:
        if self.redis is not None:
            try:
                return self.redis.zcount(self._key(key), time.time() - self.window_s, "+inf") >= self.max_failures
            except RedisError:
                pass
        return len(self._prune(key)) >= self.max_failures

    def fail(self, key: str) -> None:
        now = time.time()
        self._prune(key).append(now)
        if self.redis is not None:
            try:
                p = self.redis.pipeline(transaction=True)
                p.zadd(self._key(key), {f"{now:.6f}|{secrets.token_hex(4)}": now})
                p.zremrangebyscore(self._key(key), "-inf", now - self.window_s)
                p.expire(self._key(key), self.window_s)
                p.execute()
            except RedisError:
                pass

    def clear(self, key: str) -> None:
        self.failures.pop(key, None)
        if self.redis is not None:
            try:
                self.redis.delete(self._key(key))
            except RedisError:
                pass


@dataclass(frozen=True)
class Principal:
    name: str
    role: str  # one of ROLES, or SERVICE for an API key
    # Signed in with a password, in a role that must use two-factor sign-in, not enrolled yet:
    # may only enrol, look at who they are and sign out.
    limited: bool = False

    @property
    def is_person(self) -> bool:
        return self.role != SERVICE


def client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def audit(request: Request, who: Principal | None, action: str, target: str | None = None,
          detail: dict | None = None, actor: str | None = None) -> None:
    request.app.state.repo.audit(
        actor=actor or (who.name if who else "anonymous"), role=who.role if who else "NONE",
        action=action, target=target, detail=detail or {}, ip=client_ip(request))


def start_session(request: Request, user: dict, method: str, limited: bool = False) -> dict:
    token = secrets.token_urlsafe(32)
    expires = datetime.now(UTC) + timedelta(hours=SESSION_HOURS)
    request.app.state.repo.create_session(token_digest(token), user["username"], expires, limited, method)
    audit(request, Principal(user["username"], user["role"]), "LOGIN", detail={"method": method, "limited": limited})
    out = {"token": token, "username": user["username"], "role": user["role"], "expires_at": expires.isoformat()}
    if limited:
        out["mfa_enrolment_required"] = True
    return out


def login(request: Request, username: str, password: str) -> dict:
    st = request.app.state
    username = username.strip().lower()
    key = f"{username}|{client_ip(request)}"
    if st.login_throttle.locked(key):
        audit(request, None, "LOGIN_LOCKED", target=username, actor=username)
        raise HTTPException(status_code=429, detail="too many failed sign-ins; try again in 15 minutes")
    user = st.repo.get_user(username)
    ok = verify_password(password, user["password_hash"] if user else _DUMMY_HASH)
    if not (user and ok and user["active"]):
        st.login_throttle.fail(key)
        audit(request, None, "LOGIN_FAILED", target=username, actor=username)
        raise HTTPException(status_code=401, detail="wrong username or password")
    st.login_throttle.clear(key)
    if user.get("mfa_enabled"):
        challenge = secrets.token_urlsafe(32)
        st.repo.create_challenge(token_digest(challenge), username,
                                 datetime.now(UTC) + timedelta(minutes=CHALLENGE_MINUTES))
        audit(request, None, "LOGIN_PASSWORD_OK", target=username, actor=username)
        return {"mfa_required": True, "mfa_token": challenge, "expires_in": CHALLENGE_MINUTES * 60}
    limited = user["role"] in st.settings.mfa_required_roles
    return start_session(request, user, "password", limited=limited)


def login_second_factor(request: Request, mfa_token: str, code: str) -> dict:
    """Finish a sign-in with an authenticator code or a recovery code."""
    from . import mfa

    st = request.app.state
    challenge = st.repo.use_challenge(token_digest(mfa_token), CHALLENGE_ATTEMPTS)
    if challenge is None:
        raise HTTPException(status_code=401, detail="sign-in expired or too many wrong codes; start again")
    user = st.repo.get_user(challenge["username"])
    if not (user and user["active"] and user.get("mfa_enabled")):
        raise HTTPException(status_code=401, detail="sign-in expired; start again")
    secret = mfa.SecretBox(st.settings.secret_key).open(user["mfa_secret"] or "")
    step = mfa.match_step(secret, code, user.get("mfa_last_step")) if secret else None
    if step is not None and st.repo.set_mfa_last_step(user["username"], step):
        method = "password+totp"
    elif "-" in code and st.repo.use_recovery_code(user["username"], mfa.recovery_digest(code)):
        method = "password+recovery_code"
    else:
        audit(request, None, "MFA_FAILED", target=user["username"], actor=user["username"])
        raise HTTPException(status_code=401, detail="wrong code")
    st.repo.delete_challenge(token_digest(mfa_token))
    return start_session(request, user, method)


def current_principal(request: Request, authorization: str | None = Header(default=None),
                      x_api_key: str | None = Header(default=None)) -> Principal:
    """Who is calling. A bearer token wins over an API key if both are sent."""
    st = request.app.state
    if authorization and authorization.lower().startswith("bearer "):
        user = st.repo.get_session_user(token_digest(authorization[7:].strip()))
        if user is None:
            raise HTTPException(status_code=401, detail="session expired or invalid; sign in again")
        who = Principal(user["username"], user["role"], bool(user.get("limited")))
    elif x_api_key is not None and x_api_key in st.settings.api_keys:
        who = Principal(f"apikey:{hashlib.sha256(x_api_key.encode()).hexdigest()[:8]}", SERVICE)
    else:
        raise HTTPException(status_code=401, detail="sign in, or send a valid X-API-Key")
    if not st.limiter.allow(who.name):
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    return who


def require(*allowed: str, allow_limited: bool = False):
    """Dependency factory: the caller's role must be one of `allowed`."""

    def dependency(request: Request, authorization: str | None = Header(default=None),
                   x_api_key: str | None = Header(default=None)) -> Principal:
        who = current_principal(request, authorization, x_api_key)
        if who.limited and not allow_limited:
            raise HTTPException(status_code=403, detail="two-factor sign-in is required for your role: "
                                                        "set it up before using Foresentry")
        if who.role not in allowed:
            audit(request, who, "DENIED", target=f"{request.method} {request.url.path}")
            needed = " or ".join(a for a in allowed if a != SERVICE) or "an API key"
            raise HTTPException(status_code=403, detail=f"your role ({who.role}) cannot do this; needs {needed}")
        return who

    return dependency


can_read = require(*ROLES, SERVICE)
can_ingest = require("ADMIN", SERVICE)
# A dry-run score changes nothing, so analysts may use it to check a transaction by hand.
can_dry_run = require("ANALYST", "ADMIN", SERVICE)
can_work_alerts = require("ANALYST", "ADMIN")
admin_only = require("ADMIN")
signed_in = require(*ROLES)
signed_in_or_enrolling = require(*ROLES, allow_limited=True)
