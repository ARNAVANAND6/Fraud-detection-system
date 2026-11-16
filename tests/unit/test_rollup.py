"""Running totals must add up to what a full recount would say."""
from datetime import UTC, datetime, timedelta

from services.scorer.app import rollup


class Rec:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((" ".join(sql.split()), params))


def test_buckets_match_postgres_width_bucket():
    # width_bucket(x, 0, 1.000001, 10): 0 -> 1, 0.0999 -> 1, 0.1000001 -> 2, 1.0 -> 10
    assert [rollup.bucket(x) for x in (0.0, 0.0999, 0.1000001, 0.55, 0.9999, 1.0)] == [1, 1, 2, 6, 10, 10]


def test_a_batch_writes_one_row_per_org_and_source_in_a_fixed_order():
    t0 = datetime(2026, 11, 16, 10, 0, 30, tzinfo=UTC)
    b = rollup.Batch()
    b.add("demo", "LIVE_API", 100.0, 0.95, "CRITICAL", t0, t0)
    b.add("demo", "LIVE_API", 50.0, 0.05, "LOW", t0 + timedelta(seconds=40), t0)   # next minute
    b.add("bank-b", "LIVE_API", 10.0, 0.5, "MEDIUM", t0, t0)
    c = Rec()
    b.flush(c)
    totals = [p for sql, p in c.calls if "stats_total" in sql]
    minutes = [p for sql, p in c.calls if "stats_minute" in sql]
    assert [p[:2] for p in totals] == [("bank-b", "LIVE_API"), ("demo", "LIVE_API")]      # sorted
    demo = totals[1]
    cols = ["txns", "volume", "risk_sum", "low", "medium", "high", "critical", *(f"h{i}" for i in range(1, 11))]
    got = dict(zip(cols, demo[3:3 + len(cols)], strict=True))
    assert got["txns"] == 2 and got["volume"] == 150.0 and got["critical"] == 1 and got["low"] == 1
    assert got["h10"] == 1 and got["h1"] == 1 and got["high"] == 0
    assert len({p[2] for p in totals}) == 1                                              # one shard per batch
    assert [(p[0], p[2].minute, p[4], p[5]) for p in minutes] == [
        ("bank-b", 0, 1, 0), ("demo", 0, 1, 1), ("demo", 1, 1, 0)]
    assert not b.total and not b.minute                                                  # emptied after writing
