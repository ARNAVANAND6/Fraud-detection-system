"""State that must be shared when several API and scorer processes run side by side."""
import time

import fakeredis

from services.scorer.app import nodes
from services.scorer.app.auth import LoginThrottle
from services.scorer.app.main import RateLimiter


def test_rate_limit_is_shared_between_replicas():
    r = fakeredis.FakeRedis()
    a, b = RateLimiter(5, r), RateLimiter(5, r)
    assert [a.allow("key") for _ in range(3)] == [True] * 3
    assert [b.allow("key") for _ in range(3)] == [True, True, False]   # b sees a's three
    assert a.allow("key") is False
    assert a.allow("other-key") is True
    # Refused requests are not counted, so the key recovers when old hits leave the window.
    assert r.zcard(next(k for k in r.keys("rl:*") if r.zcard(k) == 5)) == 5


def test_rate_limit_falls_back_to_memory_without_redis():
    class Down:
        def pipeline(self, **_):
            import redis
            raise redis.ConnectionError("down")
    lim = RateLimiter(2, Down())
    assert [lim.allow("k") for _ in range(3)] == [True, True, False]


def test_login_lockout_is_shared_and_survives_a_restart():
    r = fakeredis.FakeRedis()
    first = LoginThrottle(r, max_failures=3)
    for _ in range(3):
        first.fail("admin|10.0.0.1")
    restarted_or_other_replica = LoginThrottle(r, max_failures=3)
    assert restarted_or_other_replica.locked("admin|10.0.0.1")
    assert not restarted_or_other_replica.locked("admin|10.0.0.2")
    restarted_or_other_replica.clear("admin|10.0.0.1")
    assert not first.locked("admin|10.0.0.1")
    assert not any(b"admin" in k for k in r.keys("*"))     # keys are digests, not usernames


def test_nodes_report_alive_and_stopped_processes():
    r = fakeredis.FakeRedis()
    nodes.beat(r, "api", "api-1", {"dashboard_clients": 2})
    nodes.beat(r, "scorer", "scorer-1", {"processed": 10, "partitions": 3})
    nodes.beat(r, "scorer", "scorer-2", {"processed": 7, "partitions": 3})
    nodes.leave(r, "scorer", "scorer-2")                # a clean stop, or an expired key after a crash
    seen = {(n["kind"], n["id"]): n for n in nodes.read(r)}
    assert seen[("api", "api-1")]["alive"] and seen[("api", "api-1")]["dashboard_clients"] == 2
    assert seen[("scorer", "scorer-1")]["alive"] and seen[("scorer", "scorer-1")]["partitions"] == 3
    assert seen[("scorer", "scorer-2")]["alive"] is False and "processed" not in seen[("scorer", "scorer-2")]
    assert seen[("scorer", "scorer-2")]["last_seen"].startswith(time.strftime("%Y-", time.gmtime()))


def test_nodes_not_seen_for_an_hour_are_forgotten():
    r = fakeredis.FakeRedis()
    r.zadd(nodes.INDEX, {"scorer|old": (time.time() - nodes.FORGET_AFTER_S - 5) * 1000})
    nodes.beat(r, "api", "api-1", {})
    assert [n["id"] for n in nodes.read(r)] == ["api-1"]
