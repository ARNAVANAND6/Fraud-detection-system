"""Streaming scorer: Kafka -> features (Redis) -> rules + model -> PostgreSQL -> Kafka.

Input topic depends on PIPELINE_MODE:
  spark   the topic Spark Structured Streaming validated and forwarded
  direct  the raw topic

Delivery is at-least-once. Offsets are committed only after the score is
stored, so a crash replays the last uncommitted events. Replays are harmless
because Redis history and PostgreSQL rows are keyed on transaction_id.

Retry policy:
  infrastructure errors (PostgreSQL, Redis down)  retry forever with backoff, never skip
  anything else, after 3 attempts                   dead-letter the event and move on
"""
from __future__ import annotations

import json
import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
import redis
from confluent_kafka import Consumer, KafkaError, Producer
from fraudlib import jsonlog
from fraudlib.config import get_settings
from fraudlib.features import RedisFeatureStore
from fraudlib.schemas import Source, TransactionEvent
from fraudlib.scoring import Scorer
from prometheus_client import start_http_server
from pydantic import ValidationError

from .metrics import ALERTS, EVENTS, FEATURE_SECONDS, INFERENCE_SECONDS, PIPELINE_SECONDS, RETRIES
from .repository import Repository, connect_with_retry

log = logging.getLogger("scorer")
INFRA_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError, redis.ConnectionError, redis.TimeoutError)
HEARTBEAT_KEY = "hb:scorer"
MAX_ATTEMPTS = 3


def alert_event(alert: dict, kind: str) -> dict:
    return {"type": "alert", "kind": kind, **{k: alert[k] for k in (
        "alert_id", "account_id", "priority", "status", "trigger_types", "reason_codes", "max_risk_score",
        "event_count", "assigned_to", "escalation_level", "sla_due_at", "created_at", "updated_at")}}


class ScoringWorker:
    def __init__(self) -> None:
        self.s = get_settings()
        self.s.assert_safe_for_production()
        self.redis = redis.Redis.from_url(self.s.redis_url, socket_timeout=5)
        self.store = RedisFeatureStore(self.redis)
        self.scorer = Scorer(self.s.model_dir)
        self.repo: Repository = connect_with_retry(self.s.database_url)
        self.policy = self.repo.ensure_policy()
        self.producer = Producer({"bootstrap.servers": self.s.kafka_bootstrap, "enable.idempotence": True,
                                  "acks": "all", "linger.ms": 5})
        self.consumer = Consumer({
            "bootstrap.servers": self.s.kafka_bootstrap,
            "group.id": self.s.scorer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        })
        self.running = True
        self.processed = self.duplicates = 0
        self.last_received_at: datetime | None = None
        self._last_policy = self._last_escalation = 0.0

    # ---- helpers ----------------------------------------------------------------
    def dead_letter(self, msg, error: str) -> None:
        self.producer.produce(self.s.topic_dlq, value=msg.value(), key=msg.key(), headers={
            "error": error[:500], "stage": "scorer",
            "origin": f"{msg.topic()}/{msg.partition()}/{msg.offset()}"})
        EVENTS.labels("dead_lettered").inc()
        log.warning("dead-lettered event", extra={"error": error[:300], "origin_offset": msg.offset()})

    def heartbeat(self) -> None:
        try:
            self.redis.set(HEARTBEAT_KEY, jsonlog.dumps({
                "at": datetime.now(UTC), "model_version": self.scorer.model_version,
                "processed": self.processed, "duplicates": self.duplicates,
                "last_received_at": self.last_received_at, "pipeline_mode": self.s.pipeline_mode}), ex=6)
        except redis.RedisError:
            pass

    def housekeeping(self) -> None:
        now = time.monotonic()
        if now - self._last_policy > 5:
            self.policy = self.repo.get_policy()
            self._last_policy = now
        if now - self._last_escalation > 10:
            for alert in self.repo.escalate_overdue(self.policy):
                ALERTS.labels("escalated").inc()
                log.info("alert escalated", extra={"alert_id": alert["alert_id"],
                                                   "escalation_level": alert["escalation_level"]})
                self.producer.produce(self.s.topic_scored, value=jsonlog.dumps(alert_event(alert, "ESCALATED")))
            self._last_escalation = now

    # ---- one event ---------------------------------------------------------------
    def process(self, msg) -> None:
        try:
            event = TransactionEvent.model_validate_json(msg.value())
        except (ValidationError, ValueError) as exc:
            self.dead_letter(msg, f"schema: {exc}")
            return
        source = event.source or Source.KAFKA_STREAM
        if self.s.environment == "production" and source == Source.SIMULATED_DEMO:
            self.dead_letter(msg, "simulated event rejected in the production environment")
            return
        ts_type, ts_ms = msg.timestamp()
        ingested_at = event.ingested_at or (
            datetime.fromtimestamp(ts_ms / 1000, UTC) if ts_ms and ts_ms > 0 else datetime.now(UTC))

        t0 = time.perf_counter()
        features = self.store.get_and_update(event)   # historical + behavioural features
        feature_s = time.perf_counter() - t0
        score = self.scorer.score(event, features, self.policy.thresholds)
        result = self.repo.save_scored(event, source, ingested_at, score, features,
                                       round(feature_s * 1000, 3), self.policy)
        self.last_received_at = datetime.now(UTC)
        if not result["inserted"]:
            EVENTS.labels("duplicate").inc()
            self.duplicates += 1
            return
        row, alert = result["row"], result["alert"]
        FEATURE_SECONDS.observe(feature_s)
        INFERENCE_SECONDS.observe((score.latency_ms or 0) / 1000)
        PIPELINE_SECONDS.observe(max(row["pipeline_ms"], 0) / 1000)
        EVENTS.labels("scored").inc()
        self.processed += 1
        self.producer.produce(self.s.topic_scored, key=event.transaction_id, value=jsonlog.dumps({
            "type": "transaction", "transaction_id": event.transaction_id, "source": source,
            "account_id": event.account_id, "counterparty_id": event.counterparty_id,
            "txn_type": event.txn_type, "amount": event.amount, "region": event.region,
            "event_time": event.event_time, "ingested_at": ingested_at, "scored_at": score.scored_at,
            "persisted_at": row["persisted_at"], "risk_score": score.risk_score,
            "risk_level": score.risk_level, "decision": score.decision, "reason_codes": score.reason_codes,
            "model_version": score.model_version, "inference_ms": score.latency_ms,
            "pipeline_ms": round(row["pipeline_ms"], 3),
            "alert_id": alert["alert_id"] if alert else None}))
        if alert:
            kind = "CREATED" if alert["is_new"] else "EVENT_ATTACHED"
            ALERTS.labels(kind.lower()).inc()
            self.producer.produce(self.s.topic_scored, value=jsonlog.dumps(alert_event(alert, kind)))

    def process_with_retry(self, msg) -> None:
        attempt, backoff = 0, 1.0
        while self.running:
            try:
                self.process(msg)
                return
            except INFRA_ERRORS as exc:
                RETRIES.labels("infrastructure").inc()
                log.error("dependency unavailable, retrying", extra={"error": str(exc)[:200], "wait_s": backoff})
                time.sleep(backoff)
                backoff = min(backoff * 2, 15)
            except Exception as exc:  # noqa: BLE001 - poison events must not stop the stream
                attempt += 1
                RETRIES.labels("processing").inc()
                if attempt >= MAX_ATTEMPTS:
                    log.exception("event failed after retries")
                    self.dead_letter(msg, f"processing: {type(exc).__name__}: {exc}")
                    return
                time.sleep(0.2 * attempt)

    # ---- main loop ---------------------------------------------------------------
    def run(self) -> None:
        s = self.s
        start_http_server(9101)
        self.consumer.subscribe([s.topic_scorer_input, s.topic_labels])
        log.info("scorer started", extra={"environment": s.environment, "pipeline_mode": s.pipeline_mode,
                                          "input_topic": s.topic_scorer_input,
                                          "model_version": self.scorer.model_version})
        while self.running:
            self.heartbeat()
            # Short wait: consume() returns when it has 200 messages or the timeout
            # passes, so the timeout is added latency at low volume (0.25 s here
            # measured as a 237 ms end-to-end P50 in direct mode).
            msgs = self.consumer.consume(num_messages=200, timeout=0.02)
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error", extra={"error": str(msg.error())})
                    continue
                if msg.topic() == s.topic_labels:
                    try:
                        lab = json.loads(msg.value())
                        self.repo.save_label(lab["transaction_id"], bool(lab["is_fraud"]), "SIMULATOR")
                    except (ValueError, KeyError):
                        pass
                    continue
                self.process_with_retry(msg)
            if msgs and self.running:
                self.producer.flush(10)
                self.consumer.commit(asynchronous=False)  # only after side effects are durable
            try:
                self.housekeeping()
            except INFRA_ERRORS as exc:
                log.error("housekeeping failed", extra={"error": str(exc)[:200]})
            self.producer.poll(0)
        self.consumer.close()
        self.repo.close()
        log.info("scorer stopped")


def main() -> None:
    jsonlog.setup("scorer")
    worker = ScoringWorker()

    def stop(*_):
        worker.running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    worker.run()


if __name__ == "__main__":
    main()
