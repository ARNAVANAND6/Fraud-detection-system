"""Operational alerts to a chat or paging tool through an incoming webhook.

Set ALERT_WEBHOOK_URL to a Slack, Microsoft Teams (workflow) or other incoming
webhook. The body is {"text": "..."}, which Slack and most tools accept.

Sent for: a new priority-1 fraud alert (at most 10 an hour, then one message
saying the rest are in the queue), a stopped API or scorer node, the stream
disconnecting, and a failed backup (from tools/backup.sh). Each outage
message is sent at most once per `every_s` across all replicas (a Redis key
decides who sends), so a long outage is one message, not hundreds.

Messages carry ids, scores and reason codes, never account identifiers or
amounts: the chat tool is outside the platform.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)
_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="notify")


def _post(url: str, text: str) -> None:
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as exc:  # noqa: BLE001 - an unreachable chat tool must not stop scoring
        log.warning("alert webhook failed", extra={"error": str(exc)[:200]})


def send(redis, url: str, key: str, text: str, every_s: int = 1800, post=None) -> bool:
    """Queue `text` unless a message with the same key went out in the last `every_s` seconds."""
    if not url:
        return False
    try:
        if not redis.set(f"notify:{key}", "1", nx=True, ex=every_s):
            return False
    except Exception:  # noqa: BLE001 - without Redis we cannot de-duplicate; send anyway
        pass
    if post is not None:
        post(url, text)
    else:
        _pool.submit(_post, url, text)
    return True


def send_capped(redis, url: str, text: str, limit: int = 10, window_s: int = 3600, post=None) -> bool:
    """Fraud alerts: one message each, but at most `limit` per window across all scorers.
    The message after the limit says so; the rest wait in the dashboard's queue."""
    if not url:
        return False
    try:
        bucket = f"notify:p1:{int(time.time() // window_s)}"
        n = redis.incr(bucket)
        redis.expire(bucket, window_s * 2)
    except Exception:  # noqa: BLE001
        n = 1
    if n > limit + 1:
        return False
    if n == limit + 1:
        text = (f"VYROX: {limit} priority-1 alerts already this hour; no more messages until the hour ends. "
                "The rest are in the alert queue.")
    (post or (lambda u, t: _pool.submit(_post, u, t)))(url, text)
    return True


def fraud_alert_text(alert: dict, dashboard_url: str) -> str:
    reasons = ", ".join(alert.get("reason_codes") or []) or "none"
    return (f"VYROX: new P{alert['priority']} fraud alert {alert['alert_id']}, risk score "
            f"{float(alert['max_risk_score']):.2f}, reasons: {reasons}. Review: {dashboard_url}")
