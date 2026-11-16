"""Single sign-on with OpenID Connect (Google Workspace, Microsoft Entra ID, Okta, Keycloak, ...).

Authorization-code flow with PKCE, run by the API, so the client secret never
reaches the browser:

1. /v1/auth/sso/start stores a random state, nonce and PKCE verifier (single
   use, 10 minutes) and redirects the browser to the provider.
2. The provider sends the browser back to /v1/auth/sso/callback with a code.
   The API exchanges it for an ID token, checks the token's signature against
   the provider's published keys, its issuer, audience, expiry and nonce, and
   requires a verified e-mail.
3. The person is matched to a Foresentry account by the provider's subject id, or
   by e-mail the first time. An unknown person gets an account only if their
   e-mail domain is in OIDC_ALLOWED_DOMAINS, with OIDC_DEFAULT_ROLE.
4. The browser goes back to the dashboard with the session token in the URL
   fragment (#sso_token=...), which browsers do not send to servers.

Two-factor sign-in for SSO users is the identity provider's job; Foresentry does not
ask for its own code on top.
"""
from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

import jwt
from fastapi import HTTPException, Request

from . import auth

STATE_MINUTES = 10
# Asymmetric signatures only: "none" and shared-secret (HS*) tokens are refused before any check.
ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512")
_DISCOVERY_TTL_S = 3600


class Provider:
    """The provider's endpoints (from its discovery document) and signing keys, cached."""

    def __init__(self, issuer: str, opener=urllib.request.urlopen) -> None:
        self.issuer, self._open = issuer, opener
        self._doc: dict | None = None
        self._doc_at = 0.0
        self._jwks: jwt.PyJWKSet | None = None

    def _get_json(self, url: str) -> dict:
        with self._open(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=10) as r:
            return json.load(r)

    def doc(self) -> dict:
        if self._doc is None or time.monotonic() - self._doc_at > _DISCOVERY_TTL_S:
            doc = self._get_json(f"{self.issuer}/.well-known/openid-configuration")
            if doc.get("issuer", "").rstrip("/") != self.issuer:
                raise RuntimeError("the provider's discovery document names a different issuer")
            self._doc, self._doc_at, self._jwks = doc, time.monotonic(), None
        return self._doc

    def signing_key(self, kid: str | None):
        for refresh in (False, True):       # keys rotate: reload once if the kid is unknown
            if self._jwks is None or refresh:
                self._jwks = jwt.PyJWKSet.from_dict(self._get_json(self.doc()["jwks_uri"]))
            for key in self._jwks.keys:
                if kid is None or key.key_id == kid:
                    return key
        raise HTTPException(status_code=401, detail="SSO: the token was signed with an unknown key")

    def exchange(self, code: str, verifier: str, client_id: str, client_secret: str, redirect_uri: str) -> dict:
        form = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri,
                "client_id": client_id, "code_verifier": verifier}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        if client_secret:
            basic = base64.b64encode(f"{urllib.parse.quote(client_id)}:{urllib.parse.quote(client_secret)}".encode())
            headers["Authorization"] = "Basic " + basic.decode()
        req = urllib.request.Request(self.doc()["token_endpoint"], data=urllib.parse.urlencode(form).encode(),
                                     headers=headers, method="POST")
        try:
            with self._open(req, timeout=10) as r:
                return json.load(r)
        except urllib.error.HTTPError as exc:
            raise HTTPException(status_code=401, detail=f"SSO: the provider refused the code ({exc.code})") from None


def provider(request: Request) -> Provider:
    st = request.app.state
    if getattr(st, "sso_provider", None) is None:
        st.sso_provider = Provider(st.settings.oidc_issuer)
    return st.sso_provider


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def start(request: Request) -> str:
    """Return the provider URL to send the browser to."""
    s = request.app.state.settings
    state, nonce, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(16), secrets.token_urlsafe(48)
    request.app.state.repo.save_sso_state(auth.token_digest(state), nonce, verifier,
                                          datetime.now(UTC) + timedelta(minutes=STATE_MINUTES))
    params = {
        "response_type": "code", "client_id": s.oidc_client_id, "redirect_uri": s.oidc_redirect_url,
        "scope": "openid email profile", "state": state, "nonce": nonce,
        "code_challenge": _b64url(hashlib.sha256(verifier.encode()).digest()), "code_challenge_method": "S256",
    }
    return provider(request).doc()["authorization_endpoint"] + "?" + urllib.parse.urlencode(params)


def verify_id_token(request: Request, id_token: str, nonce: str) -> dict:
    s = request.app.state.settings
    p = provider(request)
    try:
        header = jwt.get_unverified_header(id_token)
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="SSO: the ID token is malformed") from None
    if header.get("alg") not in ALGORITHMS:
        raise HTTPException(status_code=401, detail="SSO: unsigned or shared-secret ID tokens are not accepted")
    try:
        key = p.signing_key(header.get("kid"))
        claims = jwt.decode(id_token, key.key, algorithms=[header["alg"]],
                            audience=s.oidc_client_id, issuer=p.doc()["issuer"],
                            options={"require": ["exp", "iat", "iss", "aud", "sub"]}, leeway=60)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail=f"SSO: the ID token is not valid ({exc})") from None
    if not secrets.compare_digest(str(claims.get("nonce", "")), nonce):
        raise HTTPException(status_code=401, detail="SSO: nonce mismatch (replayed or mixed-up sign-in)")
    return claims


def finish(request: Request, code: str, state: str) -> dict:
    """Complete the sign-in. Returns the same session dict as a password sign-in."""
    st = request.app.state
    s = st.settings
    saved = st.repo.pop_sso_state(auth.token_digest(state))
    if saved is None:
        raise HTTPException(status_code=401, detail="SSO: sign-in expired or already used; start again")
    tokens = provider(request).exchange(code, saved["code_verifier"], s.oidc_client_id, s.oidc_client_secret,
                                        s.oidc_redirect_url)
    if "id_token" not in tokens:
        raise HTTPException(status_code=401, detail="SSO: the provider returned no ID token")
    claims = verify_id_token(request, tokens["id_token"], saved["nonce"])
    subject = f"{claims['iss']}|{claims['sub']}"
    email = claims.get("email")
    if not (email and claims.get("email_verified") is True):
        auth.audit(request, None, "SSO_REFUSED", target=email or claims["sub"], detail={"reason": "unverified e-mail"},
                   actor=email or claims["sub"])
        raise HTTPException(status_code=403, detail="SSO: your provider did not confirm a verified e-mail address")
    email = email.lower()
    user = st.repo.find_user_for_sso(subject, email)
    if user is None:
        domain = email.rsplit("@", 1)[-1]
        if domain not in s.oidc_allowed_domains:
            auth.audit(request, None, "SSO_REFUSED", target=email, detail={"reason": "no account"}, actor=email)
            raise HTTPException(status_code=403, detail="SSO: you have no Foresentry account; ask an administrator")
        username = _free_username(st.repo, email.split("@", 1)[0])
        # No usable password: this account can only sign in through the provider.
        st.repo.create_user(username, "sso-only$" + secrets.token_hex(16), s.oidc_default_role, email)
        auth.audit(request, None, "USER_CREATE", target=username,
                   detail={"role": s.oidc_default_role, "via": "sso"}, actor=email)
        user = st.repo.get_user(username)
    if not user["active"]:
        auth.audit(request, None, "SSO_REFUSED", target=user["username"], detail={"reason": "inactive"},
                   actor=user["username"])
        raise HTTPException(status_code=403, detail="SSO: your Foresentry account is deactivated")
    auth.check_org_active(request, user)
    st.repo.link_sso_subject(user["username"], subject)
    return auth.start_session(request, user, "sso")


def _free_username(repo, base: str) -> str:
    base = "".join(ch for ch in base.lower() if ch.isalnum() or ch in "._-")[:28] or "user"
    base = base if len(base) >= 3 else base + "-sso"
    name, n = base, 1
    while repo.get_user(name) is not None:
        n += 1
        name = f"{base}{n}"
    return name


def back_to_dashboard(request: Request, fragment: dict) -> str:
    return request.app.state.settings.dashboard_url + "/#" + urllib.parse.urlencode(fragment)
