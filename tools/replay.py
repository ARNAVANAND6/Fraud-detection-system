"""Event replay.

  python tools/replay.py dlq list
      Summarise the dead-letter topic by error.
  python tools/replay.py dlq replay [--error CODE] [--dry-run]
      Send dead-lettered records back to the raw topic, for example after
      fixing the producer or the validation rule that rejected them.
  python tools/replay.py range --from 2026-01-01T10:00:00+00:00 --to 2026-01-01T10:05:00+00:00 [--dry-run]
      Re-send raw-topic records from a time range (Kafka record timestamps)
      to the scorer's input topic, for example after restoring the database
      from a backup.

Replay is safe to repeat: every sink is idempotent on transaction_id, so a
record that was already processed is skipped, not double-counted.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "libs"))
from confluent_kafka import Consumer, Producer, TopicPartition
from fraudlib.config import get_settings


def read_topic(bootstrap: str, topic: str, start_ms: int | None = None, end_ms: int | None = None):
    """Yield every record currently in `topic` (optionally inside a time range).
    Uses assign(), not a consumer group, so it never disturbs the pipeline's offsets."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "vyrox-replay-tool", "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic]
        if meta.error is not None:
            raise SystemExit(f"topic {topic}: {meta.error}")
        parts = [TopicPartition(topic, p) for p in meta.partitions]
        ends = {}
        for tp in parts:
            low, high = c.get_watermark_offsets(tp, timeout=10)
            ends[tp.partition] = high
            tp.offset = low
        if start_ms is not None:
            for tp in parts:
                tp.offset = start_ms
            parts = c.offsets_for_times(parts, timeout=10)
            for tp in parts:
                if tp.offset < 0:  # nothing at or after the start time
                    tp.offset = ends[tp.partition]
        todo = {tp.partition for tp in parts if tp.offset < ends[tp.partition]}
        c.assign(parts)
        while todo:
            msg = c.poll(2.0)
            if msg is None:
                continue
            if msg.error():
                raise SystemExit(str(msg.error()))
            if msg.offset() >= ends[msg.partition()] - 1:
                todo.discard(msg.partition())
            if msg.offset() >= ends[msg.partition()]:
                continue
            ts = msg.timestamp()[1]
            if end_ms is not None and ts > end_ms:
                todo.discard(msg.partition())
                continue
            yield msg
    finally:
        c.close()


def header(msg, name: str) -> str:
    for k, v in msg.headers() or []:
        if k == name:
            return v.decode(errors="replace") if isinstance(v, bytes) else str(v)
    return "?"


def publish(bootstrap: str, topic: str, records, dry_run: bool) -> int:
    if dry_run:
        return sum(1 for _ in records)
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    n = 0
    for msg in records:
        p.produce(topic, key=msg.key(), value=msg.value(), headers=[("replayed", b"1")])
        n += 1
        if n % 1000 == 0:
            p.poll(0)
    if p.flush(30):
        raise SystemExit("Kafka did not acknowledge every replayed record; run the replay again")
    return n


def to_ms(text: str) -> int:
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise SystemExit("timestamps need a timezone offset, for example 2026-01-01T10:00:00+00:00")
    return int(dt.timestamp() * 1000)


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    dlq = sub.add_parser("dlq")
    dlq.add_argument("action", choices=["list", "replay"])
    dlq.add_argument("--error", help="only records with this error code")
    dlq.add_argument("--dry-run", action="store_true")
    rng = sub.add_parser("range")
    rng.add_argument("--from", dest="start", required=True)
    rng.add_argument("--to", dest="end", required=True)
    rng.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if a.cmd == "dlq":
        records = list(read_topic(s.kafka_bootstrap, s.topic_dlq))
        if a.action == "list":
            by = Counter((header(m, "stage"), header(m, "error").split(":")[0][:60]) for m in records)
            print(f"{len(records)} records in {s.topic_dlq}")
            for (stage, err), n in by.most_common():
                print(f"  {n:6d}  stage={stage}  error={err}")
            return
        chosen = [m for m in records if not a.error or header(m, "error").startswith(a.error)]
        n = publish(s.kafka_bootstrap, s.topic_raw, chosen, a.dry_run)
        print(f"{'would replay' if a.dry_run else 'replayed'} {n} of {len(records)} DLQ records to {s.topic_raw}")
    else:
        records = read_topic(s.kafka_bootstrap, s.topic_raw, to_ms(a.start), to_ms(a.end))
        n = publish(s.kafka_bootstrap, s.topic_scorer_input, records, a.dry_run)
        print(f"{'would replay' if a.dry_run else 'replayed'} {n} records from {s.topic_raw} to {s.topic_scorer_input}")


if __name__ == "__main__":
    main()
