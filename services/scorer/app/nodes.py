"""Which API and scorer processes are running, shared through Redis.

Every process writes `node:<kind>:<id>` every second or so with a short
expiry, and records itself in the `nodes` sorted set (score = last beat, in
milliseconds). A node whose key has expired is reported as down, and is kept
in the list for an hour so an operator can see what stopped and when.

The id is NODE_ID if set, otherwise the host name, which in Docker is the
container id and on a server is the machine name.
"""
from __future__ import annotations

import json
import os
import socket
import time
from datetime import UTC, datetime

TTL_S = 6
FORGET_AFTER_S = 3600
INDEX = "nodes"


def node_id() -> str:
    return os.environ.get("NODE_ID") or socket.gethostname()


def beat(redis, kind: str, ident: str, info: dict) -> None:
    now_ms = int(time.time() * 1000)
    p = redis.pipeline(transaction=False)
    p.set(f"node:{kind}:{ident}", json.dumps({**info, "at_ms": now_ms}, default=str), ex=TTL_S)
    p.zadd(INDEX, {f"{kind}|{ident}": now_ms})
    p.zremrangebyscore(INDEX, "-inf", now_ms - FORGET_AFTER_S * 1000)
    p.execute()


def leave(redis, kind: str, ident: str) -> None:
    """A clean shutdown says so at once instead of waiting for the key to expire."""
    redis.delete(f"node:{kind}:{ident}")


def read(redis) -> list[dict]:
    """Every node seen in the last hour, newest beat first, with `alive` and its last report."""
    members = redis.zrevrange(INDEX, 0, 199, withscores=True)
    if not members:
        return []
    names = [(m.decode() if isinstance(m, bytes) else m) for m, _ in members]
    raws = redis.mget([f"node:{n.replace('|', ':', 1)}" for n in names])
    out = []
    for name, (_, score), raw in zip(names, members, raws, strict=True):
        kind, ident = name.split("|", 1)
        info = json.loads(raw) if raw else {}
        info.pop("at_ms", None)
        out.append({"id": ident, "kind": kind, "alive": raw is not None,
                    "last_seen": _iso(score), **info})
    return out


def _iso(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()
