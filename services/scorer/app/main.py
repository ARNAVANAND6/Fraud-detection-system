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
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal

import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fraudlib import jsonlog
from fraudlib.config import Settings, get_settings
from fraudlib.features import RedisFeatureStore
from fraudlib.policy import Policy
from fraudlib.schemas import RiskScore, Source, TransactionEvent
from fraudlib.scoring import Scorer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ValidationError

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


def require_key(request: Request, x_api_key: str | None = Header(default=None)) -> str:
    s: Settings = request.app.state.settings
    if x_api_key is None or x_api_key not in s.api_keys:
        raise HTTPException(status_code=401, detail="missing or invalid X-API-Key")
    if not request.app.state.limiter.allow(x_api_key):
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    return x_api_key


def json_response(obj, status_code: int = 200) -> Response:
    return Response(jsonlog.dumps(obj), status_code=status_code, media_type="application/json")


def do_ingest(request: Request, events: list[TransactionEvent], source: Source) -> dict:
    try:
        return request.app.state.gateway.ingest(events, source)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    except (PublishError, redis.RedisError) as exc:
        raise HTTPException(status_code=503, detail=f"ingestion unavailable: {exc}") from None


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
def ingest_transaction(event: TransactionEvent, request: Request, _: str = Depends(require_key)) -> dict:
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
def import_file(file: UploadFile, request: Request, _: str = Depends(require_key)) -> dict:
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
    return {"file": file.filename, "source": Source.HISTORICAL_IMPORT.value, "rows": len(df),
            "accepted": len(result["accepted"]), "duplicates": len(result["duplicates"]),
            "invalid": invalid, "first_errors": errors}


class ScenarioRequest(BaseModel):
    scenario: Literal["RAPID_BURST", "ACCOUNT_TAKEOVER", "UNUSUAL_AMOUNT"] = "RAPID_BURST"


@app.post("/v1/demo/scenario", status_code=202)
def demo_scenario(body: ScenarioRequest, request: Request, _: str = Depends(require_key)) -> dict:
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
    return {"scenario": body.scenario, "source": Source.SIMULATED_DEMO.value, "account_id": acct,
            "transaction_ids": result["accepted"]}


@app.post("/v1/score", response_model=RiskScore)
def score(event: TransactionEvent, request: Request, _: str = Depends(require_key)) -> RiskScore:
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
async def stream(request: Request, _: str = Depends(require_key)) -> StreamingResponse:
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
def status(request: Request, _: str = Depends(require_key)) -> Response:
    return json_response(request.app.state.live.status())


@app.get("/v1/stats")
def stats(request: Request, source: Source | None = None, _: str = Depends(require_key)) -> Response:
    out = request.app.state.repo.stats(source.value if source else None)
    out["performance"] = request.app.state.live.performance()
    return json_response(out)


@app.get("/v1/performance")
def performance(request: Request, _: str = Depends(require_key)) -> Response:
    return json_response(request.app.state.live.performance())


# ---- investigation -------------------------------------------------------------------
@app.get("/v1/transactions/recent")
def recent(request: Request, limit: int = Query(50, le=500), suspicious: bool = False,
           source: Source | None = None, _: str = Depends(require_key)) -> Response:
    return json_response(request.app.state.repo.recent_transactions(
        limit, suspicious, source.value if source else None))


@app.get("/v1/transactions/{transaction_id}")
def transaction(transaction_id: str, request: Request, _: str = Depends(require_key)) -> Response:
    row = request.app.state.repo.get_transaction(transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found") from None
    return json_response(row)


class AlertUpdate(BaseModel):
    status: Literal["IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "OPEN"] | None = None
    assigned_to: str | None = None
    actor: str = "analyst"
    notes: str | None = None


@app.get("/v1/alerts")
def alerts(request: Request, status: str | None = None, limit: int = Query(100, le=500),
           _: str = Depends(require_key)) -> Response:
    return json_response(request.app.state.repo.list_alerts(status, limit))


@app.get("/v1/alerts/{alert_id}")
def alert_detail(alert_id: int, request: Request, _: str = Depends(require_key)) -> Response:
    row = request.app.state.repo.get_alert(alert_id)
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found") from None
    return json_response(row)


@app.patch("/v1/alerts/{alert_id}")
def update_alert(alert_id: int, body: AlertUpdate, request: Request, _: str = Depends(require_key)) -> Response:
    from .consumer import alert_event
    from .repository import InvalidTransition

    st = request.app.state
    try:
        row = st.repo.update_alert(alert_id, body.status, body.assigned_to, body.actor, body.notes)
    except KeyError:
        raise HTTPException(status_code=404, detail="alert not found") from None
    except InvalidTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    if hasattr(st, "gateway"):
        st.gateway.publish_update(jsonlog.dumps(alert_event(row, "UPDATED")))  # every dashboard sees it
    return json_response(row)


# ---- configuration -------------------------------------------------------------------
@app.get("/v1/config")
def get_config(request: Request, _: str = Depends(require_key)) -> Response:
    return json_response(request.app.state.repo.get_policy().model_dump(mode="json"))


@app.put("/v1/config")
def put_config(policy: Policy, request: Request, _: str = Depends(require_key)) -> Response:
    """Risk thresholds and alert settings. The scorer picks changes up within about 5 seconds."""
    saved = request.app.state.repo.set_policy(policy, "api")
    return json_response(saved.model_dump(mode="json"))
