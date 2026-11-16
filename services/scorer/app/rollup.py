"""Running totals for the dashboards, kept as payments are stored.

The dashboards used to recount the whole `predictions` table every two seconds. That is fine for
a demo and fatal at volume: in the load test those scans took half the database's CPU away from
storing payments. Now each stored payment adds itself to two small tables in the same transaction:

- `stats_total`: all-time counts per organisation and source (levels, risk histogram, volume).
- `stats_minute`: per-minute counts, for the last-30-minutes trend.

Each total is split into SHARDS rows, so several scorer processes do not queue on one hot row.
Readers add the shards up.

Existing data is counted once, when the tables are created.
"""
from __future__ import annotations

import random
from datetime import datetime

SHARDS = 16
LEVELS = ("LOW", "MEDIUM", "HIGH", "CRITICAL")

ROLLUP_SCHEMA = """
CREATE TABLE IF NOT EXISTS stats_total (
    org_id            TEXT NOT NULL,
    source            TEXT NOT NULL,
    shard             SMALLINT NOT NULL,
    txns              BIGINT NOT NULL DEFAULT 0,
    volume            DOUBLE PRECISION NOT NULL DEFAULT 0,
    risk_sum          DOUBLE PRECISION NOT NULL DEFAULT 0,
    low               BIGINT NOT NULL DEFAULT 0,
    medium            BIGINT NOT NULL DEFAULT 0,
    high              BIGINT NOT NULL DEFAULT 0,
    critical          BIGINT NOT NULL DEFAULT 0,
    h1 BIGINT NOT NULL DEFAULT 0, h2 BIGINT NOT NULL DEFAULT 0, h3 BIGINT NOT NULL DEFAULT 0,
    h4 BIGINT NOT NULL DEFAULT 0, h5 BIGINT NOT NULL DEFAULT 0, h6 BIGINT NOT NULL DEFAULT 0,
    h7 BIGINT NOT NULL DEFAULT 0, h8 BIGINT NOT NULL DEFAULT 0, h9 BIGINT NOT NULL DEFAULT 0,
    h10 BIGINT NOT NULL DEFAULT 0,
    last_persisted_at TIMESTAMPTZ,
    last_scored_at    TIMESTAMPTZ,
    PRIMARY KEY (org_id, source, shard)
);
CREATE TABLE IF NOT EXISTS stats_minute (
    org_id     TEXT NOT NULL,
    source     TEXT NOT NULL,
    minute     TIMESTAMPTZ NOT NULL,
    shard      SMALLINT NOT NULL,
    txns       BIGINT NOT NULL DEFAULT 0,
    high_risk  BIGINT NOT NULL DEFAULT 0,
    volume     DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (org_id, source, minute, shard)
);
CREATE INDEX IF NOT EXISTS stats_minute_minute_idx ON stats_minute (minute);

-- First start only: count what is already stored.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM stats_total) THEN
        INSERT INTO stats_total (org_id, source, shard, txns, volume, risk_sum, low, medium, high, critical,
                                 h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, last_persisted_at, last_scored_at)
        SELECT org_id, source, 0, count(*), coalesce(sum(amount), 0), coalesce(sum(risk_score), 0),
               count(*) FILTER (WHERE risk_level = 'LOW'), count(*) FILTER (WHERE risk_level = 'MEDIUM'),
               count(*) FILTER (WHERE risk_level = 'HIGH'), count(*) FILTER (WHERE risk_level = 'CRITICAL'),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 1),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 2),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 3),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 4),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 5),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 6),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 7),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 8),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 9),
               count(*) FILTER (WHERE width_bucket(risk_score, 0, 1.000001, 10) = 10),
               max(persisted_at), max(scored_at)
        FROM predictions GROUP BY org_id, source;
        INSERT INTO stats_minute (org_id, source, minute, shard, txns, high_risk, volume)
        SELECT org_id, source, date_trunc('minute', persisted_at), 0, count(*),
               count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')), coalesce(sum(amount), 0)
        FROM predictions WHERE persisted_at > now() - interval '60 minutes'
        GROUP BY 1, 2, 3;
    END IF;
END $$;
"""


def bucket(score: float) -> int:
    """Same buckets as width_bucket(risk_score, 0, 1.000001, 10)."""
    return min(10, max(1, int(score / 0.1000001) + 1))


class Batch:
    """Counts for the payments stored in one transaction, written once at the end.

    The whole batch goes to one shard row per organisation and source, picked at random, so two
    scorer processes rarely touch the same row, and the rows are written in a fixed order, so
    they cannot deadlock on each other.
    """

    def __init__(self) -> None:
        self.shard = random.randrange(SHARDS)
        self.total: dict[tuple[str, str], dict] = {}
        self.minute: dict[tuple[str, str, datetime], list] = {}

    def add(self, org_id: str, source: str, amount: float, risk_score: float, risk_level: str,
            persisted_at: datetime, scored_at: datetime) -> None:
        t = self.total.setdefault((org_id, source), {
            "txns": 0, "volume": 0.0, "risk_sum": 0.0, **{lv.lower(): 0 for lv in LEVELS},
            **{f"h{i}": 0 for i in range(1, 11)}, "last_persisted_at": persisted_at, "last_scored_at": scored_at})
        t["txns"] += 1
        t["volume"] += amount
        t["risk_sum"] += risk_score
        if risk_level in LEVELS:
            t[risk_level.lower()] += 1
        t[f"h{bucket(risk_score)}"] += 1
        t["last_persisted_at"] = max(t["last_persisted_at"], persisted_at)
        t["last_scored_at"] = max(t["last_scored_at"], scored_at)
        m = self.minute.setdefault((org_id, source, persisted_at.replace(second=0, microsecond=0)), [0, 0, 0.0])
        m[0] += 1
        m[1] += 1 if risk_level in ("HIGH", "CRITICAL") else 0
        m[2] += amount

    def flush(self, c) -> None:
        cols = ["txns", "volume", "risk_sum", *(lv.lower() for lv in LEVELS), *(f"h{i}" for i in range(1, 11))]
        for (org_id, source), t in sorted(self.total.items()):
            c.execute(
                f"""INSERT INTO stats_total (org_id, source, shard, {", ".join(cols)},
                                             last_persisted_at, last_scored_at)
                    VALUES (%s, %s, %s, {", ".join(["%s"] * len(cols))}, %s, %s)
                    ON CONFLICT (org_id, source, shard) DO UPDATE SET
                    {", ".join(f"{k} = stats_total.{k} + EXCLUDED.{k}" for k in cols)},
                    last_persisted_at = greatest(stats_total.last_persisted_at, EXCLUDED.last_persisted_at),
                    last_scored_at = greatest(stats_total.last_scored_at, EXCLUDED.last_scored_at)""",
                (org_id, source, self.shard, *(t[k] for k in cols), t["last_persisted_at"], t["last_scored_at"]))
        for (org_id, source, minute), (n, high, volume) in sorted(self.minute.items()):
            c.execute(
                """INSERT INTO stats_minute (org_id, source, minute, shard, txns, high_risk, volume)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (org_id, source, minute, shard) DO UPDATE SET
                       txns = stats_minute.txns + EXCLUDED.txns,
                       high_risk = stats_minute.high_risk + EXCLUDED.high_risk,
                       volume = stats_minute.volume + EXCLUDED.volume""",
                (org_id, source, minute, self.shard, n, high, volume))
        self.total.clear()
        self.minute.clear()
