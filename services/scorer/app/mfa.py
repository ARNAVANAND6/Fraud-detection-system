"""Two-factor sign-in with an authenticator app (TOTP, RFC 6238) and one-time recovery codes.

The shared secret is encrypted before it is stored (Fernet, key derived from
SECRET_KEY), so a copy of the database alone is not enough to generate codes.
A code is accepted for the current 30-second step and one step either side
(clock drift on phones), and never twice: the last step used is stored.
Recovery codes are stored only as SHA-256 digests and each works once.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
from urllib.parse import quote

from cryptography.fernet import Fernet, InvalidToken

STEP_S = 30
DIGITS = 6
DRIFT_STEPS = 1
RECOVERY_CODES = 10


def new_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def _b32(secret: str) -> bytes:
    return base64.b32decode(secret + "=" * (-len(secret) % 8), casefold=True)


def code_at(secret: str, step: int) -> str:
    digest = hmac.new(_b32(secret), struct.pack(">Q", step), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(value % 10**DIGITS).zfill(DIGITS)


def current_step(now: float | None = None) -> int:
    return int((time.time() if now is None else now) // STEP_S)


def match_step(secret: str, code: str, last_step: int | None, now: float | None = None) -> int | None:
    """The step `code` belongs to, if it is valid now and newer than `last_step`; otherwise None."""
    code = code.strip().replace(" ", "")
    if not (code.isdigit() and len(code) == DIGITS):
        return None
    step = current_step(now)
    for s in range(step - DRIFT_STEPS, step + DRIFT_STEPS + 1):
        if (last_step is None or s > last_step) and hmac.compare_digest(code_at(secret, s), code):
            return s
    return None


def otpauth_uri(secret: str, username: str, issuer: str = "Foresentry") -> str:
    label = quote(f"{issuer}:{username}")
    return f"otpauth://totp/{label}?secret={secret}&issuer={quote(issuer)}&digits={DIGITS}&period={STEP_S}"


def new_recovery_codes() -> list[str]:
    alphabet = "abcdefghjkmnpqrstuvwxyz23456789"   # no 0/o, 1/l/i
    return ["-".join("".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(2))
            for _ in range(RECOVERY_CODES)]


def recovery_digest(code: str) -> str:
    return hashlib.sha256(code.strip().lower().encode()).hexdigest()


class SecretBox:
    """Encrypts TOTP secrets at rest with a key derived from SECRET_KEY."""

    def __init__(self, secret_key: str) -> None:
        key = hashlib.sha256(b"vyrox-mfa|" + secret_key.encode()).digest()
        self._f = Fernet(base64.urlsafe_b64encode(key))

    def seal(self, secret: str) -> str:
        return self._f.encrypt(secret.encode()).decode()

    def open(self, sealed: str) -> str | None:
        try:
            return self._f.decrypt(sealed.encode()).decode()
        except InvalidToken:
            return None
