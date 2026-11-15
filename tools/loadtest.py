"""Measure how many transactions per second the scoring pipeline sustains.

Method: push a burst of simulated events into Kafka faster than the scorer can
take them, so a backlog forms, then time how fast the backlog drains. While a
backlog exists the scorer is never waiting for input, so the drain rate is its
maximum throughput on this machine with this configuration.

    python tools/loadtest.py --events 30000

Needs the stack running (Kafka, Redis, PostgreSQL, scorer, API). Demo
environment only: it writes simulated transactions. The result is printed and
written to reports/loadtest.json. It is a measurement of this machine, not a
capacity promise for another one.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def get(api: str, key: str, path: str) -> dict:
    req = urllib.request.Request(f"{api}{path}", headers={"X-API-Key": key})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=30000)
    ap.add_argument("--api", default=os.environ.get("API_URL", "http://localhost:8000"))
    ap.add_argument("--key", default=os.environ.get("API_KEY", "dev-key"))
    ap.add_argument("--burst-rate", type=int, default=5000, help="generator ticks per second during the burst")
    ap.add_argument("--timeout", type=int, default=900)
    a = ap.parse_args()

    status = get(a.api, a.key, "/v1/status")
    if status["environment"] != "demo":
        sys.exit("refusing to run: this writes simulated transactions and the environment is not demo")
    if not status["scorer_alive"]:
        sys.exit("the scorer is not running")
    start_lag = status["consumer_lag"] or 0
    start_processed = get(a.api, a.key, "/v1/stats")["processed"]

    t_burst = time.time()
    subprocess.run([sys.executable, "services/generator/generator.py", "--mode", "kafka",
                    "--rate", str(a.burst_rate), "--max-events", str(a.events)],
                   cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    burst_s = time.time() - t_burst

    # Sample stored rows and lag once a second until the backlog is gone.
    samples: list[tuple[float, int, int]] = []
    deadline = time.time() + a.timeout
    while time.time() < deadline:
        lag = get(a.api, a.key, "/v1/status")["consumer_lag"] or 0
        processed = get(a.api, a.key, "/v1/stats")["processed"]
        samples.append((time.time(), processed, lag))
        if lag <= max(50, start_lag) and len(samples) > 3:
            break
        time.sleep(1)
    else:
        sys.exit("timed out before the backlog drained")

    peak_lag = max(s[2] for s in samples)
    # Rate over the stretch where a real backlog existed (lag above 10% of its peak),
    # so the idle tail and the first sample do not dilute it.
    busy = [s for s in samples if s[2] >= 0.1 * peak_lag]
    if len(busy) < 3:
        sys.exit(f"the backlog was too small to measure (peak lag {peak_lag}); raise --events")
    seconds = busy[-1][0] - busy[0][0]
    stored = busy[-1][1] - busy[0][1]
    perf = get(a.api, a.key, "/v1/performance")
    result = {
        "measured_at": datetime.now(UTC).isoformat(),
        "events_sent": a.events,
        "burst_seconds": round(burst_s, 1),
        "burst_rate_events_per_s": round(a.events / burst_s, 1),
        "peak_backlog": peak_lag,
        "measured_window_seconds": round(seconds, 1),
        "stored_in_window": stored,
        "max_throughput_events_per_s": round(stored / seconds, 1),
        "total_stored_during_test": samples[-1][1] - start_processed,
        "pipeline_mode": status["pipeline_mode"],
        "inference_ms": perf.get("inference_ms"),
        "machine": {"cpus": os.cpu_count(), "platform": platform.platform()},
        "note": "Single scorer process, one database transaction per event. Latency during this test is "
                "dominated by queueing and is not the latency under normal load.",
    }
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "loadtest.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
