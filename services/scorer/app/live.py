"""Live layer: turns backend events into Server-Sent Events for dashboards.

A background thread consumes the `scored` topic. Events reach that topic only
after the scorer has committed them to PostgreSQL, so everything pushed to a
browser is backed by a stored record. The same thread probes Kafka every two
seconds for consumer lag, which doubles as the Kafka health check.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from collections import deque
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaException, TopicPartition
from fraudlib.config import Settings

from .gateway import percentiles
from .metrics import SSE_CLIENTS

log = logging.getLogger("live")


class Hub:
    """Fan-out to connected SSE clients. Slow clients are dropped, never buffered forever."""

    def __init__(self) -> None:
        self.clients: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self.clients.add(q)
        SSE_CLIENTS.set(len(self.clients))
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.clients.discard(q)
        SSE_CLIENTS.set(len(self.clients))

    def publish(self, event: str, data: dict) -> None:
        for q in list(self.clients):
            try:
                q.put_nowait((event, data))
            except asyncio.QueueFull:
                self.unsubscribe(q)  # its stream ends; the browser reconnects and resyncs


class LiveState:
    def __init__(self, settings: Settings, repo, redis_client, gateway) -> None:
        self.s, self.repo, self.redis, self.gateway = settings, repo, redis_client, gateway
        self.hub = Hub()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.stop = threading.Event()
        self.kafka_ok = False
        self.kafka_error: str | None = "not checked yet"
        self.kafka_checked_at = 0.0
        self.consumer_lag: int | None = None
        self.spark_backlog: int | None = None
        self.offsets: dict[str, int] = {}
        self.last_event_at: datetime | None = None
        self.last_inference_at: datetime | None = None
        self.push_ms: deque[float] = deque(maxlen=2000)  # persisted -> handed to SSE clients
        self.dirty = True
        self.latest_stats: dict | None = None
        self.scorer: dict | None = None

    # ---- Kafka side (background thread) -------------------------------------------
    def _end_offsets(self, probe: Consumer, topic: str) -> tuple[list[TopicPartition], int]:
        md = probe.list_topics(topic, timeout=2)
        t = md.topics.get(topic)
        if t is None or t.error is not None:
            raise KafkaException(t.error if t is not None else "missing topic " + topic)
        tps = [TopicPartition(topic, p) for p in t.partitions]
        return tps, sum(probe.get_watermark_offsets(tp, timeout=2, cached=False)[1] for tp in tps)

    def _probe(self, probe: Consumer) -> None:
        s = self.s
        try:
            tps, _ = self._end_offsets(probe, s.topic_scorer_input)
            lag = 0
            for tp in probe.committed(tps, timeout=2):
                lo, hi = probe.get_watermark_offsets(tp, timeout=2, cached=False)
                lag += hi - (tp.offset if tp.offset >= 0 else lo)
            ends = {name: self._end_offsets(probe, topic)[1] for name, topic in (
                ("raw", s.topic_raw), ("validated", s.topic_validated), ("dlq", s.topic_dlq))}
            self.consumer_lag, self.offsets = lag, ends
            # Approximate: Spark forwards every valid record and dead-letters the rest.
            self.spark_backlog = (max(0, ends["raw"] - ends["validated"] - ends["dlq"])
                                  if s.pipeline_mode == "spark" else None)
            self.kafka_ok, self.kafka_error = True, None
        except Exception as exc:  # noqa: BLE001 - any failure here means "Kafka not reachable"
            self.kafka_ok, self.kafka_error = False, str(exc)[:200]
            self.consumer_lag = self.spark_backlog = None
        self.kafka_checked_at = time.time()

    def _run_kafka(self) -> None:
        s = self.s
        base = {"bootstrap.servers": s.kafka_bootstrap, "enable.auto.commit": False,
                "socket.timeout.ms": 3000, "reconnect.backoff.max.ms": 2000}
        listener = Consumer({**base, "group.id": f"vyrox-api-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "latest"})
        probe = Consumer({**base, "group.id": s.scorer_group})
        listener.subscribe([s.topic_scored])
        last_probe = 0.0
        while not self.stop.is_set():
            if time.time() - last_probe > 2:
                self._probe(probe)
                last_probe = time.time()
            msg = listener.poll(0.2)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value())
            except ValueError:
                continue
            if self.loop is not None:
                self.loop.call_soon_threadsafe(self.on_backend_event, payload)
        listener.close()
        probe.close()

    # ---- event-loop side ---------------------------------------------------------------
    def on_backend_event(self, payload: dict) -> None:
        now = datetime.now(UTC)
        kind = payload.get("type")
        if kind == "transaction":
            persisted = datetime.fromisoformat(payload["persisted_at"])
            self.last_event_at = max(self.last_event_at or persisted, persisted)
            scored = datetime.fromisoformat(payload["scored_at"])
            self.last_inference_at = max(self.last_inference_at or scored, scored)
            self.push_ms.append((now - persisted).total_seconds() * 1000)
        payload["pushed_at"] = now.isoformat()
        self.dirty = True
        self.hub.publish(kind or "message", payload)

    def status(self) -> dict:
        s, now = self.s, datetime.now(UTC)
        since = (now - self.last_event_at).total_seconds() if self.last_event_at else None
        kafka_ok = self.kafka_ok and time.time() - self.kafka_checked_at < 10
        scorer_alive = self.scorer is not None
        reasons = []
        if not kafka_ok:
            state = "DISCONNECTED"
            reasons.append(f"Kafka unreachable: {self.kafka_error or 'no recent check'}")
        elif not scorer_alive:
            state = "DISCONNECTED"
            reasons.append("Scoring service heartbeat missing")
        elif (self.consumer_lag or 0) > s.lag_delayed_threshold:
            state = "DELAYED"
            reasons.append(f"Scorer is {self.consumer_lag} events behind")
        elif (self.spark_backlog or 0) > s.lag_delayed_threshold:
            state = "DELAYED"
            reasons.append(f"Spark is about {self.spark_backlog} events behind")
        elif since is None or since > s.idle_after_seconds:
            state = "IDLE"
            received = (self.scorer or {}).get("last_received_at")
            if received and (now - datetime.fromisoformat(received)).total_seconds() < s.idle_after_seconds:
                reasons.append("Events are arriving, but all of them are duplicates of stored transactions")
            else:
                reasons.append("Pipeline healthy, no events arriving")
        else:
            state = "LIVE"
        stats = self.latest_stats or {}
        return {
            "environment": s.environment, "pipeline_mode": s.pipeline_mode, "server_time": now.isoformat(),
            "stream_status": state, "reasons": reasons, "kafka_ok": kafka_ok, "scorer_alive": scorer_alive,
            "model_version": (self.scorer or {}).get("model_version"),
            "duplicates_skipped": (self.scorer or {}).get("duplicates"),
            "consumer_lag": self.consumer_lag, "spark_backlog": self.spark_backlog,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "last_inference_at": self.last_inference_at.isoformat() if self.last_inference_at else None,
            "seconds_since_last_event": round(since, 1) if since is not None else None,
            "active_sources": [r["source"] for r in stats.get("by_source", []) if r["last_minute"] > 0],
            "dashboard_clients": len(self.hub.clients),
        }

    def performance(self) -> dict:
        perf = self.repo.performance(5)
        raw, dlq = self.offsets.get("raw"), self.offsets.get("dlq")
        perf.update({
            "ingest_ms": percentiles(self.gateway.ingest_ms),   # REST/webhook request -> Kafka ack
            "push_ms": percentiles(self.push_ms),               # persisted -> sent to dashboards
            "consumer_lag": self.consumer_lag, "spark_backlog": self.spark_backlog,
            "events_received": raw, "events_dead_lettered": dlq,
            "error_rate": round(dlq / raw, 6) if raw else None,
            "measured_at": datetime.now(UTC).isoformat(),
        })
        return perf

    def _read_scorer_heartbeat(self) -> dict | None:
        try:
            raw = self.redis.get("hb:scorer")
            return json.loads(raw) if raw else None
        except Exception:  # noqa: BLE001 - Redis down means we cannot confirm the scorer
            return None

    async def _status_loop(self) -> None:
        while True:
            self.scorer = await asyncio.to_thread(self._read_scorer_heartbeat)
            self.hub.publish("status", self.status())
            await asyncio.sleep(1)

    async def _stats_loop(self) -> None:
        while True:
            # Also refresh while the last-minute rate is still above zero, so a
            # stopped stream decays to 0 TPS instead of freezing at its last value.
            if self.dirty or self.latest_stats is None or self.latest_stats["tps_last_minute"] > 0:
                self.dirty = False
                try:
                    stats = await asyncio.to_thread(self.repo.stats)
                    stats["performance"] = await asyncio.to_thread(self.performance)
                    self.latest_stats = stats
                    self.hub.publish("stats", stats)
                except Exception as exc:  # noqa: BLE001 - keep the loop alive; status shows the outage
                    log.error("stats refresh failed", extra={"error": str(exc)[:200]})
            await asyncio.sleep(2)

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        try:
            last = await asyncio.to_thread(self.repo.last_activity)
            self.last_event_at, self.last_inference_at = last["last_event_at"], last["last_inference_at"]
        except Exception:  # noqa: BLE001
            pass
        self.thread = threading.Thread(target=self._run_kafka, name="live-kafka", daemon=True)
        self.thread.start()
        self.tasks = [asyncio.create_task(self._status_loop()), asyncio.create_task(self._stats_loop())]

    async def shutdown(self) -> None:
        self.stop.set()
        for t in self.tasks:
            t.cancel()
