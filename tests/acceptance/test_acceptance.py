"""The ten VYROX acceptance tests, run against the real running stack
(Kafka, Spark or direct mode, Redis, PostgreSQL, scorer, API). Nothing is mocked.

    VYROX_ACCEPTANCE=1 pytest tests/acceptance -v

The suite stops and starts the scoring service to test disconnection. By
default it does that with docker compose; override for a host-run scorer:

    SCORER_STOP_CMD="pkill -f services.scorer.app.consumer"
    SCORER_START_CMD="python -m services.scorer.app.consumer"
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import shlex
import subprocess
import threading
import time
import uuid
from datetime import UTC, datetime

import pytest

if not os.environ.get("VYROX_ACCEPTANCE"):
    pytest.skip("needs the running stack; set VYROX_ACCEPTANCE=1", allow_module_level=True)

import httpx
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from fraudlib.config import get_settings
from psycopg.rows import dict_row

S = get_settings()
API = os.environ.get("API_URL", "http://localhost:8000")
KEY = {"X-API-Key": S.api_keys[0]}
STOP = os.environ.get("SCORER_STOP_CMD", "docker compose stop scorer")
START = os.environ.get("SCORER_START_CMD", "docker compose start scorer")
RESULTS: dict[str, object] = {}


def txn(tid: str | None = None, account: str | None = None, amount: float = 120.0, balance: float = 50_000.0,
        txn_type: str = "PAYMENT") -> dict:
    tag = uuid.uuid4().hex[:12]
    return {"transaction_id": tid or f"TXN_ACC_{tag}", "event_time": datetime.now(UTC).isoformat(),
            "account_id": account or f"C_ACC_{tag}", "counterparty_id": f"M_ACC_{tag}", "txn_type": txn_type,
            "amount": amount, "old_balance_orig": balance, "old_balance_dest": 0.0, "channel": "API",
            "device_id": f"D_{tag}"}


def risky(**kw) -> dict:
    """A transfer that drains a balance to an empty account from a new device."""
    return txn(amount=98_000.0, balance=98_500.0, txn_type="TRANSFER", **kw)


def post(event: dict) -> httpx.Response:
    return httpx.post(f"{API}/v1/transactions", json=event, headers=KEY, timeout=20)


def db():
    return psycopg.connect(S.database_url, row_factory=dict_row, autocommit=True)


def wait_for(fn, timeout: float = 30, every: float = 0.25, what: str = "condition"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(every)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def prediction(tid: str) -> dict | None:
    with db() as c:
        return c.execute("SELECT * FROM predictions WHERE transaction_id = %s", (tid,)).fetchone()


def status() -> dict:
    return httpx.get(f"{API}/v1/status", headers=KEY, timeout=10).json()


class Stream:
    """A dashboard client: one long-lived SSE connection, events collected as they are pushed."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict, float]] = []
        self._stop = threading.Event()
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        assert self._ready.wait(15), "SSE stream did not open"

    def _run(self) -> None:
        with httpx.stream("GET", f"{API}/v1/stream", headers=KEY, timeout=None) as r:
            assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
            event = "message"
            for line in r.iter_lines():
                self._ready.set()
                if self._stop.is_set():
                    return
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    self.events.append((event, json.loads(line[5:]), time.time()))

    def find(self, kind: str, pred) -> tuple[dict, float] | None:
        for k, body, at in list(self.events):
            if k == kind and pred(body):
                return body, at
        return None

    def latest(self, kind: str) -> dict | None:
        for k, body, _ in reversed(self.events):
            if k == kind:
                return body
        return None

    def close(self) -> None:
        self._stop.set()


@pytest.fixture(scope="module")
def stream():
    s = Stream()
    yield s
    s.close()


def end_offsets(topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": "vyrox-acceptance"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def find_in_topic(topic: str, start: dict[int, int], tid: str, timeout: float = 30) -> dict | None:
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": "vyrox-acceptance", "enable.auto.commit": False})
    c.assign([TopicPartition(topic, p, o) for p, o in start.items()])
    end = time.monotonic() + timeout
    try:
        while time.monotonic() < end:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            if tid.encode() in msg.value():
                return {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                        "value": json.loads(msg.value())}
        return None
    finally:
        c.close()


def scorer(cmd: str, background: bool = False) -> None:
    if background:
        subprocess.Popen(shlex.split(cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    else:
        subprocess.run(shlex.split(cmd), check=False, capture_output=True, timeout=60)


# ---------------------------------------------------------------------------------------
def test_00_stack_is_up_and_labelled():
    r = httpx.get(f"{API}/health/ready", timeout=10)
    assert r.status_code == 200, r.text
    assert r.headers["X-Vyrox-Environment"] == S.environment
    st = status()
    assert st["environment"] == S.environment and st["stream_status"] in ("LIVE", "IDLE"), st
    RESULTS["pipeline_mode"] = st["pipeline_mode"]


def test_01_and_06_transaction_reaches_the_live_feed_without_refresh(stream):
    """1: an incoming transaction appears in the live feed.
    6: the dashboard connection receives it as a server push; the client makes no further request."""
    event = txn()
    sent_at = time.time()
    assert post(event).json()["status"] == "accepted"
    body, at = wait_for(lambda: stream.find("transaction", lambda b: b["transaction_id"] == event["transaction_id"]),
                        what="transaction on the SSE stream")
    assert body["source"] == "LIVE_API" and body["amount"] == event["amount"]
    assert body["persisted_at"] and body["risk_level"] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")
    RESULTS["post_to_dashboard_push_s"] = round(at - sent_at, 3)
    # A stats push follows, computed from stored rows, also without any client request.
    wait_for(lambda: stream.find("stats", lambda b: any(k == "stats" and t > at for k, _, t in stream.events) and b),
             what="stats push after the transaction")


def test_02_transaction_passes_through_kafka():
    raw_start, val_start = end_offsets(S.topic_raw), end_offsets(S.topic_scorer_input)
    scored_start = end_offsets(S.topic_scored)
    event = txn()
    post(event)
    raw = find_in_topic(S.topic_raw, raw_start, event["transaction_id"])
    assert raw is not None, "event not found in the raw Kafka topic"
    assert raw["value"]["source"] == "LIVE_API" and raw["value"]["ingested_at"]
    hop = find_in_topic(S.topic_scorer_input, val_start, event["transaction_id"])
    assert hop is not None, f"event not found in {S.topic_scorer_input}"
    out = find_in_topic(S.topic_scored, scored_start, event["transaction_id"])
    assert out is not None, "scored event not found in the scored topic"
    RESULTS["kafka_path"] = [f"{m['topic']}[{m['partition']}]@{m['offset']}" for m in (raw, hop, out)]
    if S.pipeline_mode == "spark":
        assert raw["topic"] != hop["topic"], "in spark mode the scorer must read the Spark-forwarded topic"


def test_03_model_generates_an_actual_prediction():
    import lightgbm as lgb
    from fraudlib.features import FEATURE_NAMES

    event = risky()
    post(event)
    row = wait_for(lambda: prediction(event["transaction_id"]), what="prediction row")
    assert row["model_version"] != "rules-only", "the scorer is running without a model"
    meta = json.loads(open(f"{S.model_dir}/model_meta.json").read())
    assert row["model_version"] == meta["model_version"]
    # Recompute independently from the stored features with the model file on disk.
    booster = lgb.Booster(model_file=f"{S.model_dir}/{meta['model_file']}")
    expected = float(booster.predict([[row["features"][n] for n in FEATURE_NAMES]])[0])
    assert row["risk_score"] == pytest.approx(expected, abs=1e-5)
    assert row["risk_level"] in ("HIGH", "CRITICAL") and row["decision"] in ("REVIEW", "HOLD")
    assert row["reason_codes"] and row["scored_at"] is not None and row["inference_ms"] > 0
    RESULTS["prediction"] = {k: row[k] for k in ("risk_score", "risk_level", "decision", "reason_codes",
                                                 "model_version", "inference_ms")}
    RESULTS["risky_txn"] = event["transaction_id"]


def test_04_risk_score_is_persisted():
    event = txn()
    post(event)
    row = wait_for(lambda: prediction(event["transaction_id"]), what="prediction row")
    assert 0.0 <= row["risk_score"] <= 1.0 and row["source"] == "LIVE_API"
    assert row["persisted_at"] >= row["scored_at"] >= row["ingested_at"]
    api_row = httpx.get(f"{API}/v1/transactions/{event['transaction_id']}", headers=KEY, timeout=10).json()
    assert api_row["risk_score"] == row["risk_score"] and api_row["risk_level"] == row["risk_level"]


def test_05_qualifying_transaction_generates_an_alert(stream):
    event = risky()
    post(event)
    tid = event["transaction_id"]

    def linked():
        with db() as c:
            return c.execute("""SELECT a.* FROM alert_transactions t JOIN alerts a USING (alert_id)
                                WHERE t.transaction_id = %s""", (tid,)).fetchone()
    alert = wait_for(linked, what="alert for the risky transaction")
    assert alert["status"] == "OPEN" and alert["priority"] in (1, 2) and "RISK_SCORE" in alert["trigger_types"]
    assert alert["assigned_to"] and alert["sla_due_at"] > alert["created_at"]
    pushed, _ = wait_for(lambda: stream.find("alert", lambda b: b["alert_id"] == alert["alert_id"]),
                         what="alert pushed to the dashboard stream")
    assert pushed["kind"] == "CREATED"
    # A harmless payment must not raise one.
    calm = txn()
    post(calm)
    wait_for(lambda: prediction(calm["transaction_id"]), what="prediction row")
    with db() as c:
        assert c.execute("SELECT 1 FROM alert_transactions WHERE transaction_id = %s",
                         (calm["transaction_id"],)).fetchone() is None
    RESULTS["alert"] = {k: alert[k] for k in ("alert_id", "priority", "trigger_types", "assigned_to")}


def test_09_duplicates_create_no_duplicate_transactions_or_alerts():
    event = risky()
    tid = event["transaction_id"]
    statuses = [post(event).json()["status"] for _ in range(3)]                    # client retries
    assert statuses == ["accepted", "duplicate", "duplicate"]
    body = json.dumps({"event_id": "evt_dup", "type": "payment.created", "data": event}).encode()
    ts = str(int(time.time()))
    sig = hmac.new(S.webhook_secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    r = httpx.post(f"{API}/v1/webhooks/generic", content=body, timeout=20,
                   headers={"X-Vyrox-Timestamp": ts, "X-Vyrox-Signature": sig})    # same payment via webhook
    assert r.status_code == 202 and r.json()["status"] == "duplicate", r.text
    wait_for(lambda: prediction(tid), what="prediction row")
    # Bypass the gateway entirely: the same record delivered twice more on Kafka itself.
    p = Producer({"bootstrap.servers": S.kafka_bootstrap})
    stamped = {**event, "source": "LIVE_API", "ingested_at": datetime.now(UTC).isoformat()}
    marker = txn(account=event["account_id"])
    for _ in range(2):
        p.produce(S.topic_raw, key=event["account_id"], value=json.dumps(stamped))
    p.flush(10)
    post(marker)  # same partition, sent afterwards: once it is stored, the duplicates were processed too
    wait_for(lambda: prediction(marker["transaction_id"]), what="marker after the duplicates")
    time.sleep(1)
    with db() as c:
        n_pred = c.execute("SELECT count(*) AS n FROM predictions WHERE transaction_id = %s", (tid,)).fetchone()["n"]
        links = c.execute("SELECT alert_id FROM alert_transactions WHERE transaction_id = %s", (tid,)).fetchall()
        alerts = c.execute("SELECT alert_id, event_count FROM alerts WHERE account_id = %s",
                           (event["account_id"],)).fetchall()
        created = c.execute("""SELECT count(*) AS n FROM alert_events
                               WHERE kind IN ('CREATED', 'EVENT_ATTACHED') AND detail->>'transaction_id' = %s""",
                            (tid,)).fetchone()["n"]
    assert n_pred == 1, f"{n_pred} prediction rows for one transaction"
    assert len(links) == 1 and len(alerts) == 1 and alerts[0]["event_count"] == 1 and created == 1
    RESULTS["duplicates"] = {"deliveries": 6, "predictions": n_pred, "alerts": len(alerts)}


def test_10_latency_and_throughput_are_measured():
    event = txn()
    post(event)
    row = wait_for(lambda: prediction(event["transaction_id"]), what="prediction row")
    # The stored end-to-end latency is the difference of two recorded timestamps.
    measured = (row["persisted_at"] - row["ingested_at"]).total_seconds() * 1000
    assert row["pipeline_ms"] == pytest.approx(measured, abs=2.0)
    perf = httpx.get(f"{API}/v1/performance", headers=KEY, timeout=10).json()
    with db() as c:
        ref = c.execute("""SELECT count(*) AS n,
                                  percentile_cont(0.5) WITHIN GROUP (ORDER BY pipeline_ms) AS p50,
                                  percentile_cont(0.5) WITHIN GROUP (ORDER BY inference_ms) AS inf50
                           FROM predictions WHERE persisted_at > clock_timestamp() - interval '5 minutes'
                             AND source <> 'HISTORICAL_IMPORT'""").fetchone()
    assert perf["samples"] > 0 and ref["n"] > 0
    # The API's percentiles agree with an independent query over the same stored rows (the window moves a little).
    assert perf["pipeline_ms"]["p50"] == pytest.approx(ref["p50"], rel=0.25)
    assert perf["inference_ms"]["p50"] == pytest.approx(ref["inf50"], rel=0.25)
    for key in ("pipeline_ms", "inference_ms", "feature_ms", "ingest_ms", "push_ms"):
        v = perf[key]
        assert v is not None and 0 <= v["p50"] <= v["p95"] <= v["p99"], (key, v)
    assert perf["throughput_tps"] and perf["throughput_tps"] > 0
    assert isinstance(perf["consumer_lag"], int) and perf["error_rate"] is not None
    # Lag is the real consumer-group lag: compare with Kafka directly.
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": S.scorer_group})
    try:
        parts = [TopicPartition(S.topic_scorer_input, p) for p in end_offsets(S.topic_scorer_input)]
        lag = sum(c.get_watermark_offsets(tp, timeout=10)[1] - max(tp.offset, 0)
                  for tp in c.committed(parts, timeout=10))
    finally:
        c.close()
    assert abs(lag - httpx.get(f"{API}/v1/performance", headers=KEY, timeout=10).json()["consumer_lag"]) < 2000
    RESULTS["performance"] = perf


def test_07_and_08_disconnect_is_shown_and_reconnect_resumes(stream):
    """7: with the scoring stream down, the status says DISCONNECTED and nothing new is shown as live.
    8: after it comes back, everything accepted in the meantime is processed."""
    assert status()["stream_status"] in ("LIVE", "IDLE")
    scorer(STOP)
    t0 = time.time()
    try:
        st = wait_for(lambda: (s := status())["stream_status"] == "DISCONNECTED" and s, timeout=20,
                      what="DISCONNECTED status")
        RESULTS["seconds_to_show_disconnected"] = round(time.time() - t0, 1)
        assert st["scorer_alive"] is False and any("heartbeat" in r for r in st["reasons"])
        wait_for(lambda: (stream.latest("status") or {}).get("stream_status") == "DISCONNECTED",
                 what="DISCONNECTED pushed to the dashboard stream")
        assert httpx.get(f"{API}/health/ready", timeout=10).status_code == 503
        frozen = status()["last_event_at"]
        held = [txn() for _ in range(5)]
        assert all(post(e).json()["status"] == "accepted" for e in held)  # ingestion still accepts into Kafka
        time.sleep(4)
        assert all(prediction(e["transaction_id"]) is None for e in held)
        now = status()
        assert now["stream_status"] == "DISCONNECTED" and now["last_event_at"] == frozen  # nothing passed off as new
    finally:
        scorer(START, background=True)
    t1 = time.time()
    wait_for(lambda: all(prediction(e["transaction_id"]) for e in held), timeout=90, what="backlog to be processed")
    RESULTS["seconds_to_recover_backlog"] = round(time.time() - t1, 1)
    wait_for(lambda: status()["stream_status"] in ("LIVE", "IDLE"), timeout=60, what="status to recover")
    for e in held:
        assert stream.find("transaction", lambda b, tid=e["transaction_id"]: b["transaction_id"] == tid), \
            "recovered transaction was not pushed to the dashboard"
    with db() as c:
        n = c.execute("SELECT count(*) AS n FROM predictions WHERE transaction_id = ANY(%s)",
                      ([e["transaction_id"] for e in held],)).fetchone()["n"]
    assert n == len(held)


def test_99_write_report():
    os.makedirs("reports", exist_ok=True)
    RESULTS["run_at"] = datetime.now(UTC).isoformat()
    RESULTS["environment"] = S.environment
    with open(f"reports/acceptance_{S.pipeline_mode}.json", "w") as f:
        json.dump(RESULTS, f, indent=2, default=str)
