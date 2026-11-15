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

Not covered here, and needed before real use: single sign-on (OIDC/SAML),
multi-factor sign-in, and TLS in front of the API (tokens and passwords
travel in headers and bodies, so plain HTTP is only acceptable on localhost).
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

ROLES = ("VIEWER", "ANALYST", "ADMIN")
SERVICE = "SERVICE"  # an API key, not a person
MIN_PASSWORD_LENGTH = 10
SESSION_HOURS = 8
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
    """Locks a username + address pair after repeated failures. In-process, like RateLimiter."""

    def __init__(self, max_failures: int = 5, window_s: int = 900):
        self.max_failures, self.window_s = max_failures, window_s
        self.failures: dict[str, deque] = defaultdict(deque)

    def _prune(self, key: str) -> deque:
        q, now = self.failures[key], time.monotonic()
        while q and now - q[0] > self.window_s:
            q.popleft()
        return q

    def locked(self, key: str) -> bool:
        return len(self._prune(key)) >= self.max_failures

    def fail(self, key: str) -> None:
        self._prune(key).append(time.monotonic())

    def clear(self, key: str) -> None:
        self.failures.pop(key, None)


@dataclass(frozen=True)
class Principal:
    name: str
    role: str  # one of ROLES, or SERVICE for an API key

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
    token = secrets.token_urlsafe(32)
    expires = datetime.now(UTC) + timedelta(hours=SESSION_HOURS)
    st.repo.create_session(token_digest(token), username, expires)
    audit(request, Principal(username, user["role"]), "LOGIN")
    return {"token": token, "username": username, "role": user["role"], "expires_at": expires.isoformat()}


def current_principal(request: Request, authorization: str | None = Header(default=None),
                      x_api_key: str | None = Header(default=None)) -> Principal:
    """Who is calling. A bearer token wins over an API key if both are sent."""
    st = request.app.state
    if authorization and authorization.lower().startswith("bearer "):
        user = st.repo.get_session_user(token_digest(authorization[7:].strip()))
        if user is None:
            raise HTTPException(status_code=401, detail="session expired or invalid; sign in again")
        who = Principal(user["username"], user["role"])
    elif x_api_key is not None and x_api_key in st.settings.api_keys:
        who = Principal(f"apikey:{hashlib.sha256(x_api_key.encode()).hexdigest()[:8]}", SERVICE)
    else:
        raise HTTPException(status_code=401, detail="sign in, or send a valid X-API-Key")
    if not st.limiter.allow(who.name):
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    return who


def require(*allowed: str):
    """Dependency factory: the caller's role must be one of `allowed`."""

    def dependency(request: Request, authorization: str | None = Header(default=None),
                   x_api_key: str | None = Header(default=None)) -> Principal:
        who = current_principal(request, authorization, x_api_key)
        if who.role not in allowed:
            audit(request, who, "DENIED", target=f"{request.method} {request.url.path}")
            needed = " or ".join(a for a in allowed if a != SERVICE) or "an API key"
            raise HTTPException(status_code=403, detail=f"your role ({who.role}) cannot do this; needs {needed}")
        return who

    return dependency


can_read = require(*ROLES, SERVICE)
can_ingest = require("ADMIN", SERVICE)
can_work_alerts = require("ANALYST", "ADMIN")
admin_only = require("ADMIN")
signed_in = require(*ROLES)
