"""PostgreSQL access. A scored transaction and its alert are written in one
database transaction, keyed on transaction_id, so a replay changes nothing."""
from __future__ import annotations

import json
import time
from datetime import datetime

import psycopg
from fraudlib.policy import Policy, alert_priority, alert_triggers
from fraudlib.schemas import RiskScore, Source, TransactionEvent
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

# Allowed case transitions; anything else is rejected.
TRANSITIONS = {
    "OPEN": {"IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE"},
    "IN_REVIEW": {"CONFIRMED_FRAUD", "FALSE_POSITIVE", "OPEN"},
    "CONFIRMED_FRAUD": set(),
    "FALSE_POSITIVE": set(),
}
LIVE_SOURCES_SQL = "source <> 'HISTORICAL_IMPORT'"


class InvalidTransition(Exception):
    pass


class Repository:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 5):
        self.pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size,
                                   kwargs={"row_factory": dict_row}, open=True)

    def close(self) -> None:
        self.pool.close()

    def ping(self) -> bool:
        with self.pool.connection() as c:
            return c.execute("SELECT 1 AS ok").fetchone()["ok"] == 1

    # ---- policy ---------------------------------------------------------------
    def get_policy(self) -> Policy:
        with self.pool.connection() as c:
            row = c.execute("SELECT value FROM settings WHERE key = 'policy'").fetchone()
        return Policy.model_validate(row["value"]) if row else Policy()

    def ensure_policy(self) -> Policy:
        """Store the default policy on first start so it is visible and editable."""
        with self.pool.connection() as c:
            c.execute("INSERT INTO settings (key, value) VALUES ('policy', %s) ON CONFLICT (key) DO NOTHING",
                      (Jsonb(Policy().model_dump(mode="json")),))
        return self.get_policy()

    def set_policy(self, policy: Policy, actor: str) -> Policy:
        with self.pool.connection() as c:
            c.execute(
                """INSERT INTO settings (key, value, updated_by) VALUES ('policy', %s, %s)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,
                       updated_by = EXCLUDED.updated_by, updated_at = now()""",
                (Jsonb(policy.model_dump(mode="json")), actor))
        return policy

    # ---- the scoring write path -------------------------------------------------
    def save_scored(self, event: TransactionEvent, source: Source, ingested_at: datetime, score: RiskScore,
                    features: dict, feature_ms: float, policy: Policy) -> dict:
        """Persist one scored transaction and run the alert engine.

        Returns {"inserted": bool, "row": prediction fields, "alert": alert row or None}.
        """
        with self.pool.connection() as c, c.transaction():
            row = c.execute(
                """INSERT INTO predictions (transaction_id, source, account_id, counterparty_id, txn_type,
                       amount, region, event_time, ingested_at, scored_at, risk_score, risk_level, decision,
                       reason_codes, model_version, features, feature_ms, inference_ms, pipeline_ms)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           extract(epoch FROM clock_timestamp() - %s::timestamptz) * 1000)
                   ON CONFLICT (transaction_id) DO NOTHING
                   RETURNING persisted_at, pipeline_ms""",
                (event.transaction_id, source.value, event.account_id, event.counterparty_id,
                 event.txn_type.value, event.amount, event.region, event.event_time, ingested_at,
                 score.scored_at, score.risk_score, score.risk_level.value, score.decision.value,
                 score.reason_codes, score.model_version, json.dumps(features), feature_ms,
                 score.latency_ms, ingested_at),
            ).fetchone()
            if row is None:
                return {"inserted": False, "row": None, "alert": None}
            triggers = alert_triggers(policy, score.risk_level, score.rule_categories)
            alert = self._raise_alert(c, event, score, triggers, policy) if triggers else None
            return {"inserted": True, "row": row, "alert": alert}

    def _raise_alert(self, c, event: TransactionEvent, score: RiskScore, triggers: list[str],
                     policy: Policy) -> dict:
        ap = policy.alerts
        priority = alert_priority(score.risk_level, triggers)
        # Serialise alert handling per account, so two events for one account
        # can never both decide "no open alert yet".
        c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (event.account_id,))
        existing = c.execute(
            """SELECT alert_id, priority, event_count FROM alerts
               WHERE account_id = %s AND status IN ('OPEN', 'IN_REVIEW')
                 AND last_event_at > clock_timestamp() - make_interval(mins => %s)
               ORDER BY created_at DESC LIMIT 1 FOR UPDATE""",
            (event.account_id, ap.dedup_minutes)).fetchone()
        if existing:
            # Deduplicate: attach to the open alert; repeated activity raises its priority.
            new_priority = min(existing["priority"], priority)
            if existing["event_count"] + 1 >= 3:
                new_priority = max(1, min(new_priority, existing["priority"] - 1))
            alert = c.execute(
                """UPDATE alerts SET event_count = event_count + 1,
                       max_risk_score = greatest(max_risk_score, %s), priority = %s,
                       total_amount = total_amount + %s,
                       trigger_types = ARRAY(SELECT DISTINCT unnest(trigger_types || %s::text[])),
                       reason_codes = ARRAY(SELECT DISTINCT unnest(reason_codes || %s::text[])),
                       last_event_at = clock_timestamp(), updated_at = clock_timestamp()
                   WHERE alert_id = %s RETURNING *""",
                (score.risk_score, new_priority, event.amount, triggers, score.reason_codes,
                 existing["alert_id"])).fetchone()
            kind = "EVENT_ATTACHED"
        else:
            assignee = c.execute(
                """SELECT a.name FROM unnest(%s::text[]) WITH ORDINALITY AS a(name, ord)
                   LEFT JOIN alerts al ON al.assigned_to = a.name AND al.status IN ('OPEN', 'IN_REVIEW')
                   GROUP BY a.name, a.ord ORDER BY count(al.alert_id), a.ord LIMIT 1""",
                (ap.analysts,)).fetchone()
            alert = c.execute(
                """INSERT INTO alerts (account_id, first_transaction, priority, trigger_types, reason_codes,
                       max_risk_score, total_amount, assigned_to, sla_due_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s, clock_timestamp() + make_interval(secs => %s))
                   RETURNING *""",
                (event.account_id, event.transaction_id, priority, triggers, score.reason_codes,
                 score.risk_score, event.amount, assignee["name"] if assignee else None,
                 60 * ap.sla_minutes.get(str(priority), 60))).fetchone()
            kind = "CREATED"
        c.execute("INSERT INTO alert_transactions (transaction_id, alert_id, trigger_types) VALUES (%s,%s,%s)",
                  (event.transaction_id, alert["alert_id"], triggers))
        c.execute("INSERT INTO alert_events (alert_id, kind, detail, actor) VALUES (%s,%s,%s,'alert-engine')",
                  (alert["alert_id"], kind, Jsonb({"transaction_id": event.transaction_id, "triggers": triggers,
                                                   "risk_score": score.risk_score})))
        alert["is_new"] = kind == "CREATED"
        return alert

    def escalate_overdue(self, policy: Policy) -> list[dict]:
        """Escalate OPEN alerts nobody acknowledged before their SLA. Atomic, so
        several workers can call it without escalating the same alert twice."""
        ap = policy.alerts
        with self.pool.connection() as c, c.transaction():
            rows = c.execute(
                """UPDATE alerts SET escalation_level = escalation_level + 1, escalated_at = clock_timestamp(),
                       priority = greatest(priority - 1, 1), assigned_to = %s,
                       sla_due_at = clock_timestamp() + make_interval(secs => 60 * coalesce(
                           (%s::jsonb ->> greatest(priority - 1, 1)::text)::float, 60)),
                       updated_at = clock_timestamp()
                   WHERE status = 'OPEN' AND sla_due_at < clock_timestamp() AND escalation_level < %s
                   RETURNING *""",
                (ap.supervisor, json.dumps(ap.sla_minutes), ap.max_escalations)).fetchall()
            for r in rows:
                c.execute(
                    "INSERT INTO alert_events (alert_id, kind, detail, actor) "
                    "VALUES (%s,'ESCALATED',%s,'alert-engine')",
                    (r["alert_id"], Jsonb({"level": r["escalation_level"], "assigned_to": ap.supervisor})))
        return rows

    def save_label(self, transaction_id: str, is_fraud: bool, source: str) -> None:
        with self.pool.connection() as c:
            c.execute(
                """INSERT INTO labels (transaction_id, is_fraud, source) VALUES (%s,%s,%s)
                   ON CONFLICT (transaction_id) DO UPDATE
                   SET is_fraud = EXCLUDED.is_fraud, source = EXCLUDED.source, labelled_at = now()
                   WHERE labels.source <> 'ANALYST' OR EXCLUDED.source = 'ANALYST'""",
                (transaction_id, is_fraud, source))

    # ---- case management --------------------------------------------------------
    def list_alerts(self, status: str | None, limit: int) -> list[dict]:
        with self.pool.connection() as c:
            q = """SELECT a.*, p.source, p.txn_type, p.amount AS first_amount,
                          a.sla_due_at < clock_timestamp() AND a.status = 'OPEN' AS sla_breached
                   FROM alerts a JOIN predictions p ON p.transaction_id = a.first_transaction"""
            params: list = []
            if status:
                q += " WHERE a.status = %s"
                params.append(status)
            q += " ORDER BY a.priority, a.created_at DESC LIMIT %s"
            params.append(limit)
            return c.execute(q, params).fetchall()

    def get_alert(self, alert_id: int) -> dict | None:
        with self.pool.connection() as c:
            alert = c.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,)).fetchone()
            if alert is None:
                return None
            alert["transactions"] = c.execute(
                """SELECT p.transaction_id, p.source, p.txn_type, p.amount, p.counterparty_id, p.event_time,
                          p.risk_score, p.risk_level, p.reason_codes, t.trigger_types
                   FROM alert_transactions t JOIN predictions p USING (transaction_id)
                   WHERE t.alert_id = %s ORDER BY p.persisted_at DESC LIMIT 100""", (alert_id,)).fetchall()
            alert["events"] = c.execute(
                "SELECT kind, detail, actor, at FROM alert_events WHERE alert_id = %s ORDER BY id DESC LIMIT 100",
                (alert_id,)).fetchall()
            return alert

    def update_alert(self, alert_id: int, to_status: str | None, assign_to: str | None, actor: str,
                     notes: str | None) -> dict:
        with self.pool.connection() as c, c.transaction():
            cur = c.execute("SELECT * FROM alerts WHERE alert_id = %s FOR UPDATE", (alert_id,)).fetchone()
            if cur is None:
                raise KeyError(alert_id)
            if to_status and to_status != cur["status"] and to_status not in TRANSITIONS[cur["status"]]:
                raise InvalidTransition(f"{cur['status']} -> {to_status} is not allowed")
            row = c.execute(
                """UPDATE alerts SET status = coalesce(%s, status), assigned_to = coalesce(%s, assigned_to),
                       notes = coalesce(%s, notes), updated_at = clock_timestamp()
                   WHERE alert_id = %s RETURNING *""",
                (to_status, assign_to, notes, alert_id)).fetchone()
            if to_status and to_status != cur["status"]:
                c.execute("INSERT INTO alert_events (alert_id, kind, detail, actor) VALUES (%s,'STATUS',%s,%s)",
                          (alert_id, Jsonb({"from": cur["status"], "to": to_status}), actor))
            if assign_to and assign_to != cur["assigned_to"]:
                c.execute("INSERT INTO alert_events (alert_id, kind, detail, actor) VALUES (%s,'ASSIGNED',%s,%s)",
                          (alert_id, Jsonb({"from": cur["assigned_to"], "to": assign_to}), actor))
            if to_status in ("CONFIRMED_FRAUD", "FALSE_POSITIVE"):
                # The analyst's verdict becomes the label for every transaction in the alert.
                c.execute(
                    """INSERT INTO labels (transaction_id, is_fraud, source)
                       SELECT transaction_id, %s, 'ANALYST' FROM alert_transactions WHERE alert_id = %s
                       ON CONFLICT (transaction_id) DO UPDATE SET is_fraud = EXCLUDED.is_fraud,
                           source = 'ANALYST', labelled_at = now()""",
                    (to_status == "CONFIRMED_FRAUD", alert_id))
        return row

    def get_transaction(self, transaction_id: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute(
                """SELECT p.*, t.alert_id, a.status AS alert_status, a.priority AS alert_priority,
                          l.is_fraud AS label, l.source AS label_source
                   FROM predictions p LEFT JOIN alert_transactions t USING (transaction_id)
                   LEFT JOIN alerts a USING (alert_id) LEFT JOIN labels l USING (transaction_id)
                   WHERE p.transaction_id = %s""", (transaction_id,)).fetchone()

    FEED_COLUMNS = """transaction_id, source, account_id, counterparty_id, txn_type, amount, region, event_time,
                      ingested_at, scored_at, persisted_at, risk_score, risk_level, decision, reason_codes,
                      model_version, inference_ms, pipeline_ms"""

    def recent_transactions(self, limit: int, suspicious_only: bool = False, source: str | None = None) -> list[dict]:
        where, params = [], []
        if suspicious_only:
            where.append("risk_level IN ('HIGH', 'CRITICAL')")
        if source:
            where.append("source = %s")
            params.append(source)
        q = f"SELECT {self.FEED_COLUMNS} FROM predictions"
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY persisted_at DESC LIMIT %s"
        with self.pool.connection() as c:
            return c.execute(q, [*params, limit]).fetchall()

    def last_activity(self) -> dict:
        with self.pool.connection() as c:
            return c.execute(
                "SELECT max(persisted_at) AS last_event_at, max(scored_at) AS last_inference_at FROM predictions"
            ).fetchone()

    # ---- analytics, all computed from stored rows -----------------------------------
    def stats(self, source: str | None = None) -> dict:
        f, params = ("WHERE source = %s", [source]) if source else ("", [])
        and_f = f.replace("WHERE", "AND")
        with self.pool.connection() as c:
            totals = c.execute(
                f"""SELECT count(*) AS processed, coalesce(sum(amount), 0)::float AS total_volume,
                           count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS high_risk,
                           count(*) FILTER (WHERE risk_level = 'CRITICAL') AS critical,
                           coalesce(avg(risk_score), 0) AS avg_risk_score,
                           max(persisted_at) AS last_event_at, max(scored_at) AS last_inference_at
                    FROM predictions {f}""", params).fetchone()
            minute = c.execute(
                f"""SELECT count(*) AS n FROM predictions
                    WHERE persisted_at > clock_timestamp() - interval '60 seconds' {and_f}""", params).fetchone()
            levels = c.execute(
                f"SELECT risk_level, count(*) AS n FROM predictions {f} GROUP BY 1", params).fetchall()
            hist = c.execute(
                f"""SELECT width_bucket(risk_score, 0, 1.000001, 10) AS bucket, count(*) AS n
                    FROM predictions {f} GROUP BY 1 ORDER BY 1""", params).fetchall()
            trend = c.execute(
                f"""SELECT date_trunc('minute', persisted_at) AS minute, count(*) AS txns,
                           count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS high_risk,
                           coalesce(sum(amount), 0)::float AS volume
                    FROM predictions WHERE persisted_at > clock_timestamp() - interval '30 minutes' {and_f}
                    GROUP BY 1 ORDER BY 1""", params).fetchall()
            sources = c.execute(
                """SELECT source, count(*) AS txns, coalesce(sum(amount), 0)::float AS volume,
                          count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS high_risk,
                          max(persisted_at) AS last_event_at,
                          count(*) FILTER (WHERE persisted_at > clock_timestamp() - interval '60 seconds')
                            AS last_minute
                   FROM predictions GROUP BY source ORDER BY txns DESC""").fetchall()
            alerts = c.execute(
                """SELECT count(*) AS total,
                          count(*) FILTER (WHERE status = 'OPEN') AS open,
                          count(*) FILTER (WHERE status = 'IN_REVIEW') AS in_review,
                          count(*) FILTER (WHERE status = 'CONFIRMED_FRAUD') AS confirmed,
                          count(*) FILTER (WHERE status = 'FALSE_POSITIVE') AS false_positive,
                          count(*) FILTER (WHERE escalation_level > 0 AND status IN ('OPEN','IN_REVIEW')) AS escalated,
                          count(*) FILTER (WHERE status = 'OPEN' AND sla_due_at < clock_timestamp()) AS sla_breached
                   FROM alerts""").fetchone()
            quality = c.execute(
                f"""SELECT count(*) AS labelled,
                           count(*) FILTER (WHERE l.is_fraud AND p.risk_level IN ('HIGH','CRITICAL')) AS tp,
                           count(*) FILTER (WHERE NOT l.is_fraud AND p.risk_level IN ('HIGH','CRITICAL')) AS fp,
                           count(*) FILTER (WHERE l.is_fraud AND p.risk_level NOT IN ('HIGH','CRITICAL')) AS fn
                    FROM labels l JOIN predictions p USING (transaction_id)
                        {f.replace("source", "p.source")}""", params).fetchone()
        return {
            "source_filter": source,
            "processed": totals["processed"],
            "tps_last_minute": round(minute["n"] / 60, 2),
            "total_volume": totals["total_volume"],
            "high_risk": totals["high_risk"],
            "critical": totals["critical"],
            "avg_risk_score": round(float(totals["avg_risk_score"]), 5),
            "last_event_at": totals["last_event_at"],
            "last_inference_at": totals["last_inference_at"],
            "risk_levels": {r["risk_level"]: r["n"] for r in levels},
            "risk_histogram": [{"bucket": r["bucket"], "n": r["n"]} for r in hist],
            "trend": [{"minute": r["minute"].isoformat(), "txns": r["txns"], "high_risk": r["high_risk"],
                       "volume": r["volume"]} for r in trend],
            "by_source": sources,
            "alerts": alerts,
            "label_quality": quality,
        }

    def performance(self, window_minutes: int = 5) -> dict:
        """Measured latency percentiles over the last N minutes of stored rows.

        Historical imports are excluded from latency: they are bulk-loaded, so
        their queueing time says nothing about live processing.
        """
        with self.pool.connection() as c:
            row = c.execute(
                f"""SELECT count(*) AS samples,
                       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY pipeline_ms) AS pipeline,
                       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY inference_ms) AS inference,
                       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY feature_ms) AS feature,
                       min(persisted_at) AS first_at, max(persisted_at) AS last_at
                    FROM predictions
                    WHERE persisted_at > clock_timestamp() - make_interval(mins => %s) AND {LIVE_SOURCES_SQL}""",
                (window_minutes,)).fetchone()

        def pct(arr):
            return None if not arr or arr[0] is None else {
                "p50": round(arr[0], 3), "p95": round(arr[1], 3), "p99": round(arr[2], 3)}

        span = (row["last_at"] - row["first_at"]).total_seconds() if row["samples"] else 0
        return {
            "window_minutes": window_minutes,
            "samples": row["samples"],
            "throughput_tps": round(row["samples"] / span, 2) if span > 0 else None,
            "pipeline_ms": pct(row["pipeline"]),     # ingested -> persisted (end to end)
            "inference_ms": pct(row["inference"]),   # rules + model
            "feature_ms": pct(row["feature"]),       # Redis read + update
        }


def connect_with_retry(dsn: str, attempts: int = 30) -> Repository:
    for i in range(attempts):
        try:
            repo = Repository(dsn)
            repo.ping()
            return repo
        except psycopg.OperationalError:
            time.sleep(min(2 ** i, 5))
    raise RuntimeError("database not reachable")
