"""Ingestion gateway: validate -> idempotency check -> publish to Kafka.

Every entry point (REST, webhook, file import, demo scenario) goes through
`Gateway.ingest`, which stamps the source label and the ingestion time. The
caller can never set those two fields.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from collections import deque
from datetime import UTC, datetime

from confluent_kafka import Producer
from fraudlib.config import Settings
from fraudlib.schemas import Source, TransactionEvent

from .metrics import INGEST_SECONDS, INGESTED

log = logging.getLogger("gateway")
IDEMPOTENCY_TTL_S = 24 * 3600
WEBHOOK_TOLERANCE_S = 300


class PublishError(Exception):
    pass


class Gateway:
    def __init__(self, settings: Settings, redis_client) -> None:
        self.s = settings
        self.redis = redis_client
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True,
                                  "acks": "all", "linger.ms": 0, "message.timeout.ms": 10000})
        self.ingest_ms: deque[float] = deque(maxlen=2000)  # measured request -> Kafka ack

    def ingest(self, events: list[TransactionEvent], source: Source) -> dict:
        """Returns {"accepted": [...ids], "duplicates": [...ids]}. Raises PublishError
        if Kafka did not acknowledge; nothing is then recorded as seen."""
        if self.s.environment == "production" and source == Source.SIMULATED_DEMO:
            raise PermissionError("simulated events are not accepted in production")
        t0 = time.perf_counter()
        pipe = self.redis.pipeline(transaction=False)
        for e in events:
            pipe.set(f"idem:{e.transaction_id}", source.value, nx=True, ex=IDEMPOTENCY_TTL_S)
        fresh_flags = pipe.execute()
        accepted, duplicates, failed = [], [], []
        seen_in_batch: set[str] = set()

        def on_delivery(err, msg):
            if err is not None:
                failed.append(msg.key().decode() if msg.key() else "?")

        now = datetime.now(UTC)
        for e, fresh in zip(events, fresh_flags, strict=True):
            if not fresh or e.transaction_id in seen_in_batch:
                duplicates.append(e.transaction_id)
                continue
            seen_in_batch.add(e.transaction_id)
            stamped = e.model_copy(update={"source": source, "ingested_at": now})
            self.producer.produce(self.s.topic_raw, key=e.account_id, value=stamped.model_dump_json(),
                                  headers={"transaction_id": e.transaction_id}, on_delivery=on_delivery)
            accepted.append(e.transaction_id)
            if len(accepted) % 5000 == 0:
                self.producer.poll(0)
        remaining = self.producer.flush(15)
        if failed or remaining:
            # Forget the idempotency keys so the caller can safely retry.
            pipe = self.redis.pipeline(transaction=False)
            for tid in accepted:
                pipe.delete(f"idem:{tid}")
            pipe.execute()
            INGESTED.labels(source.value, "publish_failed").inc(len(accepted))
            raise PublishError("Kafka did not acknowledge the events; retry the request")
        elapsed = time.perf_counter() - t0
        if len(events) == 1:
            self.ingest_ms.append(elapsed * 1000)
            INGEST_SECONDS.labels(source.value).observe(elapsed)
        INGESTED.labels(source.value, "accepted").inc(len(accepted))
        INGESTED.labels(source.value, "duplicate").inc(len(duplicates))
        return {"accepted": accepted, "duplicates": duplicates}

    def publish_update(self, payload: str) -> None:
        self.producer.produce(self.s.topic_scored, value=payload)
        self.producer.poll(0)


def verify_webhook(secret: str, timestamp: str | None, signature: str | None, body: bytes) -> str | None:
    """HMAC-SHA256 over "<timestamp>.<raw body>". Returns an error message or None."""
    if not secret:
        return "webhook secret is not configured"
    if not timestamp or not signature:
        return "missing X-Vyrox-Timestamp or X-Vyrox-Signature"
    try:
        skew = abs(time.time() - float(timestamp))
    except ValueError:
        return "bad timestamp"
    if skew > WEBHOOK_TOLERANCE_S:
        return "timestamp outside the allowed window (replay protection)"
    expected = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature.removeprefix("sha256=")):
        return "signature mismatch"
    return None


def percentiles(values) -> dict | None:
    xs = sorted(values)
    if not xs:
        return None

    def at(q: float) -> float:
        return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3)

    return {"p50": at(0.50), "p95": at(0.95), "p99": at(0.99), "samples": len(xs)}
