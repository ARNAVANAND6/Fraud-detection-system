"""VYROX API: ingestion gateway, live stream, investigation and configuration.

Ingestion  POST /v1/transactions          authenticated REST          -> LIVE_API
           POST /v1/webhooks/generic      HMAC-signed webhook         -> LIVE_WEBHOOK
           POST /v1/imports               CSV / Parquet upload        -> HISTORICAL_IMPORT
           POST /v1/demo/scenario         demo environment only       -> SIMULATED_DEMO
Live       GET  /v1/stream                Server-Sent Events, fed by stored backend events
Run this with a single worker: the SSE hub lives in process memory.
"""
from __future__ import annotations

import asyncio
import io
import json
import random
import re
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal

import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from fraudlib import drift, jsonlog
from fraudlib.config import Settings, get_settings
from fraudlib.features import RedisFeatureStore
from fraudlib.policy import Policy
from fraudlib.schemas import RiskScore, Source, TransactionEvent
from fraudlib.scoring import Scorer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ValidationError

from . import auth, mfa, sso
from .auth import Principal, admin_only, can_ingest, can_read, can_work_alerts, signed_in, signed_in_or_enrolling
from .gateway import Gateway, PublishError, verify_webhook
from .metrics import API_LATENCY

MAX_IMPORT_BYTES = 50 * 1024 * 1024
MAX_IMPORT_ROWS = 500_000


class RateLimiter:
    """Sliding-window limit per API key. In-process only: with several API
    replicas this would move to Redis (INCR + EXPIRE)."""

    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self.hits: dict[str, deque] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        q = self.hits[key]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= self.per_minute:
            return False
        q.append(now)
        return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    s: Settings = get_settings()
    s.assert_safe_for_production()
    st = app.state
    st.settings = s
    st.limiter = RateLimiter(s.rate_limit_per_minute)
    st.login_throttle = auth.LoginThrottle()
    injected = hasattr(st, "repo")  # unit tests inject fakes and skip Kafka
    if not injected:
        from .live import LiveState
        from .repository import connect_with_retry

        jsonlog.setup("api")
        st.redis = redis.Redis.from_url(s.redis_url, socket_timeout=2)
        st.store = RedisFeatureStore(st.redis)
        st.scorer = Scorer(s.model_dir)
        st.repo = connect_with_retry(s.database_url)
        st.repo.ensure_policy()
        st.repo.ensure_auth(s.admin_username, auth.hash_password(s.admin_password))
        st.gateway = Gateway(s, st.redis)
        st.live = LiveState(s, st.repo, st.redis, st.gateway)
        await st.live.start()
    yield
    if not injected:
        await st.live.shutdown()
        st.repo.close()


app = FastAPI(title="VYROX Risk API", version="0.2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=list(get_settings().cors_origins),
                   allow_methods=["GET", "POST", "PATCH", "PUT"], allow_headers=["*"])


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", "unmatched")
    if path != "/v1/stream":
        API_LATENCY.labels(path).observe(time.perf_counter() - t0)
    response.headers["X-Vyrox-Environment"] = request.app.state.settings.environment
    return response


def json_response(obj, status_code: int = 200) -> Response:
    return Response(jsonlog.dumps(obj), status_code=status_code, media_type="application/json")


def do_ingest(request: Request, events: list[TransactionEvent], source: Source) -> dict:
    try:
        return request.app.state.gateway.ingest(events, source)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    except (PublishError, redis.RedisError) as exc:
        raise HTTPException(status_code=503, detail=f"ingestion unavailable: {exc}") from None


# ---- sign-in, users, audit ---------------------------------------------------------
class LoginRequest(BaseModel):
    username: str
    password: str


class NewUser(BaseModel):
    username: str
    password: str
    role: Literal["VIEWER", "ANALYST", "ADMIN"]
    email: str | None = None   # lets this person sign in with single sign-on


class UserChange(BaseModel):
    role: Literal["VIEWER", "ANALYST", "ADMIN"] | None = None
    active: bool | None = None
    password: str | None = None
    reset_mfa: bool = False    # for someone who lost their phone and recovery codes
    email: str | None = None


class SecondFactor(BaseModel):
    mfa_token: str
    code: str


class MfaCode(BaseModel):
    code: str


class MfaDisable(BaseModel):
    password: str
    code: str


EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")


def clean_email(email: str | None) -> str | None:
    if email is None or not email.strip():
        return None
    email = email.strip().lower()
    if not EMAIL_RE.fullmatch(email):
        raise HTTPException(status_code=422, detail="email: not an e-mail address")
    return email


@app.post("/v1/auth/login")
def login(body: LoginRequest, request: Request) -> Response:
    return json_response(auth.login(request, body.username, body.password))


@app.post("/v1/auth/login/mfa")
def login_mfa(body: SecondFactor, request: Request) -> Response:
    """Second step of a sign-in for people with two-factor sign-in: authenticator code or recovery code."""
    return json_response(auth.login_second_factor(request, body.mfa_token, body.code))


@app.get("/v1/auth/mfa")
def mfa_status(request: Request, who: Principal = Depends(signed_in_or_enrolling)) -> dict:
    user = request.app.state.repo.get_user(who.name)
    return {"enabled": bool(user and user.get("mfa_enabled")),
            "required": who.role in request.app.state.settings.mfa_required_roles,
            "recovery_codes_left": len(user.get("mfa_recovery") or []) if user else 0}


@app.post("/v1/auth/mfa/setup")
def mfa_setup(request: Request, who: Principal = Depends(signed_in_or_enrolling)) -> dict:
    """Start enrolling an authenticator app. Nothing changes until /enable confirms a code."""
    st = request.app.state
    user = st.repo.get_user(who.name)
    if user.get("mfa_enabled"):
        raise HTTPException(status_code=409, detail="two-factor sign-in is already on; turn it off first")
    secret = mfa.new_secret()
    st.repo.set_mfa_pending(who.name, mfa.SecretBox(st.settings.secret_key).seal(secret))
    return {"secret": secret, "otpauth_uri": mfa.otpauth_uri(secret, who.name)}


@app.post("/v1/auth/mfa/enable")
def mfa_enable(body: MfaCode, request: Request, who: Principal = Depends(signed_in_or_enrolling)) -> dict:
    st = request.app.state
    user = st.repo.get_user(who.name)
    secret = mfa.SecretBox(st.settings.secret_key).open(user.get("mfa_pending") or "")
    step = mfa.match_step(secret, body.code, None) if secret else None
    if step is None:
        raise HTTPException(status_code=400, detail="that code does not match; check the phone's clock and try again")
    codes = mfa.new_recovery_codes()
    st.repo.enable_mfa(who.name, [mfa.recovery_digest(c) for c in codes], step)
    auth.audit(request, who, "MFA_ENABLED", target=who.name)
    return {"recovery_codes": codes}


@app.post("/v1/auth/mfa/disable", status_code=204)
def mfa_disable(body: MfaDisable, request: Request, who: Principal = Depends(signed_in)) -> Response:
    st = request.app.state
    if who.role in st.settings.mfa_required_roles:
        raise HTTPException(status_code=403, detail="your role must use two-factor sign-in")
    user = st.repo.get_user(who.name)
    secret = mfa.SecretBox(st.settings.secret_key).open(user.get("mfa_secret") or "")
    if not (auth.verify_password(body.password, user["password_hash"]) and secret
            and mfa.match_step(secret, body.code, user.get("mfa_last_step")) is not None):
        auth.audit(request, who, "MFA_DISABLE_FAILED", target=who.name)
        raise HTTPException(status_code=401, detail="wrong password or code")
    st.repo.disable_mfa(who.name)
    auth.audit(request, who, "MFA_DISABLED", target=who.name)
    return Response(status_code=204)


@app.get("/v1/auth/sso")
def sso_config(request: Request) -> dict:
    s = request.app.state.settings
    return {"enabled": s.sso_enabled, "provider_name": s.oidc_provider_name if s.sso_enabled else None}


@app.get("/v1/auth/sso/start")
def sso_start(request: Request) -> Response:
    if not request.app.state.settings.sso_enabled:
        raise HTTPException(status_code=404, detail="single sign-on is not configured")
    return RedirectResponse(sso.start(request), status_code=302)


@app.get("/v1/auth/sso/callback")
def sso_callback(request: Request, code: str | None = None, state: str | None = None,
                 error: str | None = None, error_description: str | None = None) -> Response:
    """The provider sends the browser here. The browser always ends up back on the dashboard."""
    if not request.app.state.settings.sso_enabled:
        raise HTTPException(status_code=404, detail="single sign-on is not configured")
    if error or not (code and state):
        message = error_description or error or "the provider sent no code"
        return RedirectResponse(sso.back_to_dashboard(request, {"sso_error": message[:200]}), status_code=302)
    try:
        session = sso.finish(request, code, state)
    except HTTPException as exc:
        return RedirectResponse(sso.back_to_dashboard(request, {"sso_error": str(exc.detail)}), status_code=302)
    return RedirectResponse(sso.back_to_dashboard(request, {"sso_token": session["token"]}), status_code=302)


@app.post("/v1/auth/logout", status_code=204)
def logout(request: Request, who: Principal = Depends(signed_in_or_enrolling),
           authorization: str | None = Header(default=None)) -> Response:
    request.app.state.repo.delete_session(auth.token_digest(authorization[7:].strip()))
    auth.audit(request, who, "LOGOUT")
    return Response(status_code=204)


@app.get("/v1/auth/me")
def me(who: Principal = Depends(signed_in_or_enrolling)) -> dict:
    out = {"username": who.name, "role": who.role}
    if who.limited:
        out["mfa_enrolment_required"] = True
    return out


@app.post("/v1/auth/password", status_code=204)
def change_own_password(body: LoginRequest, request: Request, who: Principal = Depends(signed_in)) -> Response:
    """Change your own password. `username` is ignored; `password` is the new one. Signs out every session."""
    auth.check_password_rules(body.password)
    request.app.state.repo.update_user(who.name, None, None, auth.hash_password(body.password))
    auth.audit(request, who, "PASSWORD_CHANGE", target=who.name)
    return Response(status_code=204)


@app.get("/v1/users")
def list_users(request: Request, _: Principal = Depends(admin_only)) -> Response:
    return json_response(request.app.state.repo.list_users())


@app.post("/v1/users", status_code=201)
def create_user(body: NewUser, request: Request, who: Principal = Depends(admin_only)) -> Response:
    username = body.username.strip().lower()
    if not re.fullmatch(r"[a-z0-9._-]{3,32}", username):
        raise HTTPException(status_code=422, detail="username: 3 to 32 of a-z, 0-9, dot, dash, underscore")
    auth.check_password_rules(body.password)
    email = clean_email(body.email)
    if not request.app.state.repo.create_user(username, auth.hash_password(body.password), body.role, email):
        raise HTTPException(status_code=409, detail="that username or e-mail already exists")
    auth.audit(request, who, "USER_CREATE", target=username, detail={"role": body.role, "email": email})
    return json_response({"username": username, "role": body.role, "active": True, "email": email,
                          "mfa_enabled": False, "sso": False}, 201)


@app.patch("/v1/users/{username}")
def change_user(username: str, body: UserChange, request: Request,
                who: Principal = Depends(admin_only)) -> Response:
    if body.password is not None:
        auth.check_password_rules(body.password)
    try:
        row = request.app.state.repo.update_user(
            username, body.role, body.active, auth.hash_password(body.password) if body.password else None,
            reset_mfa=body.reset_mfa, email=clean_email(body.email))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")
    auth.audit(request, who, "USER_CHANGE", target=username,
               detail={"role": body.role, "active": body.active, "password_reset": body.password is not None,
                       "mfa_reset": body.reset_mfa, "email": body.email})
    return json_response(row)


@app.get("/v1/audit")
def audit_log(request: Request, limit: int = Query(200, le=1000),
              _: Principal = Depends(admin_only)) -> Response:
    return json_response(request.app.state.repo.list_audit(limit))


# ---- health and metrics ----------------------------------------------------------
@app.get("/health/live")
def live() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
def ready(request: Request) -> Response:
    st = request.app.state
    checks = {}
    try:
        checks["redis"] = bool(st.store.r.ping())
    except Exception:  # noqa: BLE001
        checks["redis"] = False
    try:
        checks["postgres"] = st.repo.ping()
    except Exception:  # noqa: BLE001
        checks["postgres"] = False
    live_state = getattr(st, "live", None)
    if live_state is not None:
        status = live_state.status()
        checks["kafka"] = status["kafka_ok"]
        checks["scorer"] = status["scorer_alive"]
    ok = all(checks.values())
    return json_response({"status": "ok" if ok else "degraded", "checks": checks,
                          "environment": st.settings.environment,
                          "model_version": st.scorer.model_version}, 200 if ok else 503)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---- ingestion ---------------------------------------------------------------------
@app.post("/v1/transactions", status_code=202)
def ingest_transaction(event: TransactionEvent, request: Request, who: Principal = Depends(can_ingest)) -> dict:
    """Authenticated REST ingestion. Safe to retry: the same transaction_id is accepted once."""
    result = do_ingest(request, [event], Source.LIVE_API)
    return {"transaction_id": event.transaction_id, "source": Source.LIVE_API.value,
            "status": "accepted" if result["accepted"] else "duplicate"}


class WebhookEnvelope(BaseModel):
    event_id: str
    type: str
    data: TransactionEvent


@app.post("/v1/webhooks/generic", status_code=202)
async def webhook(request: Request, x_vyrox_timestamp: str | None = Header(default=None),
                  x_vyrox_signature: str | None = Header(default=None)) -> dict:
    """Signed webhook in VYROX's own envelope. A real payment gateway needs an
    adapter that maps its payload to this schema; none is bundled or claimed."""
    body = await request.body()
    s: Settings = request.app.state.settings
    problem = verify_webhook(s.webhook_secret, x_vyrox_timestamp, x_vyrox_signature, body)
    if problem:
        raise HTTPException(status_code=503 if "not configured" in problem else 401, detail=problem)
    try:
        envelope = WebhookEnvelope.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=json.loads(exc.json(include_url=False))) from None
    result = await asyncio.to_thread(do_ingest, request, [envelope.data], Source.LIVE_WEBHOOK)
    return {"event_id": envelope.event_id, "transaction_id": envelope.data.transaction_id,
            "source": Source.LIVE_WEBHOOK.value, "status": "accepted" if result["accepted"] else "duplicate"}


@app.post("/v1/imports", status_code=202)
def import_file(file: UploadFile, request: Request, who: Principal = Depends(admin_only)) -> dict:
    """Historical CSV or Parquet upload. Columns must match the transaction schema.
    Rows are scored by the same pipeline and labelled HISTORICAL_IMPORT."""
    import pandas as pd

    raw = file.file.read(MAX_IMPORT_BYTES + 1)
    if len(raw) > MAX_IMPORT_BYTES:
        raise HTTPException(status_code=413, detail="file larger than 50 MB")
    name = (file.filename or "").lower()
    try:
        if name.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(raw))
        elif name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(raw))
        else:
            raise HTTPException(status_code=415, detail="only .csv and .parquet files are supported")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - any parse failure is a bad upload
        raise HTTPException(status_code=400, detail=f"could not read file: {exc}") from None
    if len(df) > MAX_IMPORT_ROWS:
        raise HTTPException(status_code=413, detail=f"more than {MAX_IMPORT_ROWS} rows")
    allowed = set(TransactionEvent.model_fields) - {"source", "ingested_at"}
    df = df[[c for c in df.columns if c in allowed]]
    events, errors = [], []
    for i, rec in enumerate(df.to_dict("records")):
        rec = {k: v for k, v in rec.items() if not (isinstance(v, float) and v != v)}  # drop NaN
        if hasattr(rec.get("event_time"), "isoformat"):
            rec["event_time"] = rec["event_time"].isoformat()
        try:
            events.append(TransactionEvent.model_validate(rec))
        except ValidationError as exc:
            if len(errors) < 20:
                first = exc.errors(include_url=False)[0]
                errors.append({"row": i + 1, "field": ".".join(map(str, first["loc"])), "error": first["msg"]})
    invalid = len(df) - len(events)
    result = do_ingest(request, events, Source.HISTORICAL_IMPORT) if events else {"accepted": [], "duplicates": []}
    auth.audit(request, who, "IMPORT", target=file.filename,
               detail={"rows": len(df), "accepted": len(result["accepted"])})
    return {"file": file.filename, "source": Source.HISTORICAL_IMPORT.value, "rows": len(df),
            "accepted": len(result["accepted"]), "duplicates": len(result["duplicates"]),
            "invalid": invalid, "first_errors": errors}


class ScenarioRequest(BaseModel):
    scenario: Literal["RAPID_BURST", "ACCOUNT_TAKEOVER", "UNUSUAL_AMOUNT"] = "RAPID_BURST"


@app.post("/v1/demo/scenario", status_code=202)
def demo_scenario(body: ScenarioRequest, request: Request, who: Principal = Depends(admin_only)) -> dict:
    """Inject a controlled fraud scenario. Exists only in the demo environment."""
    if request.app.state.settings.environment != "demo":
        raise HTTPException(status_code=404, detail="not available outside the demo environment")
    now, tag = datetime.now(UTC), uuid.uuid4().hex[:10]
    acct, mule, device = f"DEMO{random.randrange(10**6):06d}", f"M{random.randrange(10**9):09d}", f"D{tag}"
    base = {"account_id": acct, "counterparty_id": mule, "txn_type": "TRANSFER", "channel": "APP",
            "region": "DEMO", "device_id": device, "old_balance_dest": 0.0}
    if body.scenario == "RAPID_BURST":
        specs = [(now - timedelta(seconds=3 * (5 - k)), 9000.0 + 500 * k, 60000.0 - 9000 * k) for k in range(6)]
    elif body.scenario == "ACCOUNT_TAKEOVER":
        specs = [(now, 48000.0, 48500.0)]
    else:
        specs = [(now, 250000.0, 400000.0)]
    events = [TransactionEvent(transaction_id=f"TXN_SCN_{tag}_{i}", event_time=t, amount=amt,
                               old_balance_orig=bal, **base) for i, (t, amt, bal) in enumerate(specs)]
    result = do_ingest(request, events, Source.SIMULATED_DEMO)
    auth.audit(request, who, "DEMO_SCENARIO", target=body.scenario, detail={"account_id": acct})
    return {"scenario": body.scenario, "source": Source.SIMULATED_DEMO.value, "account_id": acct,
            "transaction_ids": result["accepted"]}


@app.post("/v1/score", response_model=RiskScore)
def score(event: TransactionEvent, request: Request, who: Principal = Depends(can_ingest)) -> RiskScore:
    """Dry run: scores with current features without storing anything or changing feature state."""
    st = request.app.state
    try:
        features = st.store.get_features(event)
        policy = st.repo.get_policy()
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="feature store unavailable") from None
    return st.scorer.score(event, features, policy.thresholds)


# ---- live stream -------------------------------------------------------------------
@app.get("/v1/stream")
async def stream(request: Request, _: Principal = Depends(can_read)) -> StreamingResponse:
    live_state = request.app.state.live
    hub = live_state.hub
    q = hub.subscribe()

    async def gen():
        try:
            yield f"event: status\ndata: {jsonlog.dumps(live_state.status())}\n\n"
            if live_state.latest_stats is not None:
                yield f"event: stats\ndata: {jsonlog.dumps(live_state.latest_stats)}\n\n"
            while q in hub.clients:
                try:
                    event, data = await asyncio.wait_for(q.get(), timeout=5)
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield f"event: {event}\ndata: {jsonlog.dumps(data)}\n\n"
        finally:
            hub.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/v1/status")
def status(request: Request, _: Principal = Depends(can_read)) -> Response:
    return json_response(request.app.state.live.status())


@app.get("/v1/stats")
def stats(request: Request, source: Source | None = None, _: Principal = Depends(can_read)) -> Response:
    out = request.app.state.repo.stats(source.value if source else None)
    out["performance"] = request.app.state.live.performance()
    return json_response(out)


@app.get("/v1/performance")
def performance(request: Request, _: Principal = Depends(can_read)) -> Response:
    return json_response(request.app.state.live.performance())


@app.get("/v1/model/drift")
def model_drift(request: Request, _: Principal = Depends(can_read)) -> Response:
    """Live feature distributions against the model's training data (PSI). Recomputed at most once a minute."""
    st = request.app.state
    cached = getattr(st, "drift_cache", None)
    if cached and time.monotonic() - cached[0] < 60:
        return json_response(cached[1])
    out = {"model_version": st.scorer.model_version, "measured_at": datetime.now(UTC).isoformat()}
    if st.scorer.reference is None:
        out.update({"status": "NO_REFERENCE", "features": []})
    else:
        rows, span = st.repo.recent_features()
        out.update(drift.compare(st.scorer.reference, rows, span))
        out["reference_data"] = st.scorer.reference.get("data")
    st.drift_cache = (time.monotonic(), out)
    return json_response(out)


# ---- investigation -------------------------------------------------------------------
@app.get("/v1/transactions/recent")
def recent(request: Request, limit: int = Query(50, le=500), suspicious: bool = False,
           source: Source | None = None, _: Principal = Depends(can_read)) -> Response:
    return json_response(request.app.state.repo.recent_transactions(
        limit, suspicious, source.value if source else None))


@app.get("/v1/transactions/{transaction_id}")
def transaction(transaction_id: str, request: Request, _: Principal = Depends(can_read)) -> Response:
    row = request.app.state.repo.get_transaction(transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found") from None
    return json_response(row)


class AlertUpdate(BaseModel):
    status: Literal["IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "OPEN"] | None = None
    assigned_to: str | None = None
    notes: str | None = None


@app.get("/v1/alerts")
def alerts(request: Request, status: str | None = None, limit: int = Query(100, le=500),
           _: Principal = Depends(can_read)) -> Response:
    return json_response(request.app.state.repo.list_alerts(status, limit))


@app.get("/v1/alerts/{alert_id}")
def alert_detail(alert_id: int, request: Request, _: Principal = Depends(can_read)) -> Response:
    row = request.app.state.repo.get_alert(alert_id)
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found") from None
    return json_response(row)


@app.patch("/v1/alerts/{alert_id}")
def update_alert(alert_id: int, body: AlertUpdate, request: Request,
                 who: Principal = Depends(can_work_alerts)) -> Response:
    from .consumer import alert_event
    from .repository import InvalidTransition

    st = request.app.state
    try:
        row = st.repo.update_alert(alert_id, body.status, body.assigned_to, who.name, body.notes)
    except KeyError:
        raise HTTPException(status_code=404, detail="alert not found") from None
    except InvalidTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    if hasattr(st, "gateway"):
        st.gateway.publish_update(jsonlog.dumps(alert_event(row, "UPDATED")))  # every dashboard sees it
    auth.audit(request, who, "ALERT_UPDATE", target=str(alert_id),
               detail=body.model_dump(exclude_none=True))
    return json_response(row)


# ---- configuration -------------------------------------------------------------------
@app.get("/v1/config")
def get_config(request: Request, _: Principal = Depends(can_read)) -> Response:
    return json_response(request.app.state.repo.get_policy().model_dump(mode="json"))


@app.put("/v1/config")
def put_config(policy: Policy, request: Request, who: Principal = Depends(admin_only)) -> Response:
    """Risk thresholds and alert settings. The scorer picks changes up within about 5 seconds."""
    before = request.app.state.repo.get_policy().model_dump(mode="json")
    saved = request.app.state.repo.set_policy(policy, who.name)
    after = saved.model_dump(mode="json")
    auth.audit(request, who, "CONFIG_CHANGE", target="policy",
               detail={"changed": {k: {"from": before[k], "to": after[k]} for k in after if before.get(k) != after[k]}})
    return json_response(saved.model_dump(mode="json"))
