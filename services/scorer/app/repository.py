"""PostgreSQL access. A scored transaction and its alert are written in one
database transaction, keyed on transaction_id, so a replay changes nothing."""
from __future__ import annotations

import json
import math
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


def _org(event: TransactionEvent) -> str:
    return event.org_id or "demo"


def _json_safe(features: dict) -> dict:
    """PostgreSQL JSON has no NaN: a missing feature (unknown balance) is stored as null."""
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in features.items()}


class Repository:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 5):
        self.pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size,
                                   kwargs={"row_factory": dict_row}, open=True)

    def close(self) -> None:
        self.pool.close()

    def ping(self) -> bool:
        with self.pool.connection() as c:
            return c.execute("SELECT 1 AS ok").fetchone()["ok"] == 1

    # ---- users, sessions, audit ------------------------------------------------
    def ensure_schema(self, c=None) -> None:
        """Bring an older database up to date: auth tables, organisations. Safe to run at every start."""
        from .auth import AUTH_SCHEMA
        from .tenancy import TENANCY_SCHEMA

        if c is None:
            with self.pool.connection() as conn, conn.transaction():
                return self.ensure_schema(conn)
        # API and scorer replicas can start at once; one at a time changes the schema.
        c.execute("SELECT pg_advisory_xact_lock(7461)")
        c.execute(AUTH_SCHEMA)
        c.execute(TENANCY_SCHEMA)

    def ensure_auth(self, admin_username: str, admin_password_hash: str,
                    superadmin_username: str | None = None, superadmin_password_hash: str | None = None) -> None:
        """Schema, the demo organisation's first admin, and the platform's first superadmin."""
        with self.pool.connection() as c, c.transaction():
            self.ensure_schema(c)
            c.execute("INSERT INTO users (username, password_hash, role, org_id) VALUES (%s, %s, 'ADMIN', 'demo') "
                      "ON CONFLICT (username) DO NOTHING", (admin_username, admin_password_hash))
            if superadmin_username and superadmin_password_hash:
                c.execute("INSERT INTO users (username, password_hash, role, org_id) "
                          "VALUES (%s, %s, 'SUPERADMIN', NULL) ON CONFLICT (username) DO NOTHING",
                          (superadmin_username, superadmin_password_hash))
            c.execute("DELETE FROM sessions WHERE expires_at < now()")

    def get_user(self, username: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM users WHERE username = %s", (username,)).fetchone()

    USER_COLUMNS = """username, role, org_id, account_ids, active, email, mfa_enabled,
                      sso_subject IS NOT NULL AS sso, created_at, updated_at"""

    def list_users(self, org: str | None = None) -> list[dict]:
        with self.pool.connection() as c:
            if org:
                return c.execute(f"SELECT {self.USER_COLUMNS} FROM users WHERE org_id = %s ORDER BY username",
                                 (org,)).fetchall()
            return c.execute(f"SELECT {self.USER_COLUMNS} FROM users ORDER BY org_id NULLS FIRST, username").fetchall()

    def create_user(self, username: str, password_hash: str, role: str, email: str | None = None,
                    org_id: str | None = "demo", account_ids: list[str] | tuple = ()) -> bool:
        with self.pool.connection() as c:
            try:
                cur = c.execute("INSERT INTO users (username, password_hash, role, email, org_id, account_ids) "
                                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (username) DO NOTHING",
                                (username, password_hash, role, email, None if role == "SUPERADMIN" else org_id,
                                 list(account_ids)))
            except psycopg.errors.UniqueViolation:
                return False   # e-mail already used by another account
            return cur.rowcount == 1

    def update_user(self, username: str, role: str | None, active: bool | None,
                    password_hash: str | None, reset_mfa: bool = False, email: str | None = None,
                    org: str | None = None, account_ids: list[str] | None = None) -> dict | None:
        """Change a user (only within `org` when given). Refuses to leave an organisation, or the
        platform, without an active administrator."""
        with self.pool.connection() as c, c.transaction():
            cur = c.execute("SELECT * FROM users WHERE username = %s FOR UPDATE", (username,)).fetchone()
            if cur is None or (org and cur["org_id"] != org):
                return None
            for top in ("ADMIN", "SUPERADMIN"):
                loses = cur["role"] == top and cur["active"] and ((role is not None and role != top) or active is False)
                if loses:
                    others = c.execute(
                        """SELECT count(*) AS n FROM users WHERE role = %s AND active AND username <> %s
                           AND org_id IS NOT DISTINCT FROM %s""", (top, username, cur["org_id"])).fetchone()["n"]
                    if others == 0:
                        raise ValueError("this is the last active administrator" if top == "ADMIN"
                                         else "this is the last active superadmin")
            row = c.execute(
                f"""UPDATE users SET role = coalesce(%s, role), active = coalesce(%s, active),
                       password_hash = coalesce(%s, password_hash), email = coalesce(%s, email),
                       account_ids = coalesce(%s, account_ids),
                       mfa_enabled = mfa_enabled AND NOT %s,
                       mfa_secret = CASE WHEN %s THEN NULL ELSE mfa_secret END,
                       mfa_recovery = CASE WHEN %s THEN '{{}}' ELSE mfa_recovery END,
                       updated_at = now()
                   WHERE username = %s
                   RETURNING {self.USER_COLUMNS}""",
                (role, active, password_hash, email, account_ids, reset_mfa, reset_mfa, reset_mfa,
                 username)).fetchone()
            if (active is False or password_hash is not None or reset_mfa
                    or (role is not None and role != cur["role"])):
                c.execute("DELETE FROM sessions WHERE username = %s", (username,))  # sign them out everywhere
            return row

    def create_session(self, token_sha256: str, username: str, expires_at: datetime,
                       limited: bool = False, method: str = "password") -> None:
        with self.pool.connection() as c:
            c.execute("INSERT INTO sessions (token_sha256, username, expires_at, limited, method) "
                      "VALUES (%s, %s, %s, %s, %s)", (token_sha256, username, expires_at, limited, method))

    # ---- two-factor sign-in ----
    def create_challenge(self, token_sha256: str, username: str, expires_at: datetime) -> None:
        with self.pool.connection() as c:
            c.execute("DELETE FROM login_challenges WHERE expires_at < now()")
            c.execute("INSERT INTO login_challenges (token_sha256, username, expires_at) VALUES (%s, %s, %s)",
                      (token_sha256, username, expires_at))

    def use_challenge(self, token_sha256: str, max_attempts: int) -> dict | None:
        """Count one attempt against a pending sign-in; None if it is unknown, expired or used up."""
        with self.pool.connection() as c:
            return c.execute(
                """UPDATE login_challenges SET attempts = attempts + 1
                   WHERE token_sha256 = %s AND expires_at > now() AND attempts < %s
                   RETURNING username, attempts""", (token_sha256, max_attempts)).fetchone()

    def delete_challenge(self, token_sha256: str) -> None:
        with self.pool.connection() as c:
            c.execute("DELETE FROM login_challenges WHERE token_sha256 = %s", (token_sha256,))

    def set_mfa_last_step(self, username: str, step: int) -> bool:
        """Record a used code's time step. False if that step (or a later one) was already used."""
        with self.pool.connection() as c:
            return c.execute(
                """UPDATE users SET mfa_last_step = %s
                   WHERE username = %s AND (mfa_last_step IS NULL OR mfa_last_step < %s)""",
                (step, username, step)).rowcount == 1

    def use_recovery_code(self, username: str, digest: str) -> bool:
        with self.pool.connection() as c:
            return c.execute(
                """UPDATE users SET mfa_recovery = array_remove(mfa_recovery, %s)
                   WHERE username = %s AND mfa_enabled AND %s = ANY (mfa_recovery)""",
                (digest, username, digest)).rowcount == 1

    def set_mfa_pending(self, username: str, sealed_secret: str) -> None:
        with self.pool.connection() as c:
            c.execute("UPDATE users SET mfa_pending = %s WHERE username = %s", (sealed_secret, username))

    def enable_mfa(self, username: str, recovery_digests: list[str], step: int) -> None:
        """Make the pending secret the active one. Other sessions end: they were not made with it."""
        with self.pool.connection() as c, c.transaction():
            c.execute("""UPDATE users SET mfa_secret = mfa_pending, mfa_pending = NULL, mfa_enabled = TRUE,
                             mfa_recovery = %s, mfa_last_step = %s, updated_at = now()
                         WHERE username = %s""", (recovery_digests, step, username))
            c.execute("UPDATE sessions SET limited = FALSE WHERE username = %s", (username,))

    def disable_mfa(self, username: str) -> None:
        with self.pool.connection() as c:
            c.execute("""UPDATE users SET mfa_enabled = FALSE, mfa_secret = NULL, mfa_pending = NULL,
                             mfa_recovery = '{}', updated_at = now() WHERE username = %s""", (username,))

    # ---- single sign-on ----
    def find_user_for_sso(self, subject: str, email: str | None) -> dict | None:
        with self.pool.connection() as c:
            row = c.execute("SELECT * FROM users WHERE sso_subject = %s", (subject,)).fetchone()
            if row is None and email:
                row = c.execute("SELECT * FROM users WHERE lower(email) = lower(%s)", (email,)).fetchone()
            return row

    def link_sso_subject(self, username: str, subject: str) -> None:
        with self.pool.connection() as c:
            c.execute("UPDATE users SET sso_subject = %s WHERE username = %s AND sso_subject IS NULL",
                      (subject, username))

    def save_sso_state(self, state_sha256: str, nonce: str, code_verifier: str, expires_at: datetime) -> None:
        with self.pool.connection() as c:
            c.execute("DELETE FROM sso_states WHERE expires_at < now()")
            c.execute("INSERT INTO sso_states (state_sha256, nonce, code_verifier, expires_at) VALUES (%s,%s,%s,%s)",
                      (state_sha256, nonce, code_verifier, expires_at))

    def pop_sso_state(self, state_sha256: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute("DELETE FROM sso_states WHERE state_sha256 = %s AND expires_at > now() "
                             "RETURNING nonce, code_verifier", (state_sha256,)).fetchone()

    def get_session_user(self, token_sha256: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute(
                """SELECT u.username, u.role, u.org_id, u.account_ids, s.limited, o.status AS org_status
                   FROM sessions s JOIN users u USING (username)
                   LEFT JOIN organisations o ON o.org_id = u.org_id
                   WHERE s.token_sha256 = %s AND s.expires_at > now() AND u.active""", (token_sha256,)).fetchone()

    def delete_session(self, token_sha256: str) -> None:
        with self.pool.connection() as c:
            c.execute("DELETE FROM sessions WHERE token_sha256 = %s", (token_sha256,))

    def audit(self, actor: str, role: str, action: str, target: str | None, detail: dict, ip: str | None,
              org_id: str | None = None) -> None:
        with self.pool.connection() as c:
            c.execute("INSERT INTO audit_log (actor, role, action, target, detail, ip, org_id) "
                      "VALUES (%s,%s,%s,%s,%s,%s,%s)", (actor, role, action, target, Jsonb(detail), ip, org_id))

    def list_audit(self, limit: int, org: str | None = None) -> list[dict]:
        with self.pool.connection() as c:
            if org:
                return c.execute("SELECT * FROM audit_log WHERE org_id = %s ORDER BY id DESC LIMIT %s",
                                 (org, limit)).fetchall()
            return c.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT %s", (limit,)).fetchall()

    # ---- organisations and their API keys ----------------------------------------
    def get_org(self, org_id: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute("SELECT * FROM organisations WHERE org_id = %s", (org_id,)).fetchone()

    def org_active(self, org_id: str) -> bool:
        with self.pool.connection() as c:
            row = c.execute("SELECT status FROM organisations WHERE org_id = %s", (org_id,)).fetchone()
        return bool(row) and row["status"] == "ACTIVE"

    def list_orgs(self) -> list[dict]:
        """Every organisation with its usage, measured from stored rows."""
        with self.pool.connection() as c:
            return c.execute(
                """SELECT o.org_id, o.name, o.region, o.status, o.created_at,
                          (SELECT count(*) FROM users u WHERE u.org_id = o.org_id) AS users,
                          (SELECT count(*) FROM predictions p WHERE p.org_id = o.org_id
                             AND p.persisted_at > now() - interval '24 hours') AS transactions_24h,
                          (SELECT count(*) FROM alerts a WHERE a.org_id = o.org_id
                             AND a.status IN ('OPEN', 'IN_REVIEW')) AS open_alerts,
                          (SELECT max(persisted_at) FROM predictions p WHERE p.org_id = o.org_id) AS last_event_at
                   FROM organisations o ORDER BY o.created_at""").fetchall()

    def create_org(self, org_id: str, name: str, region: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute("INSERT INTO organisations (org_id, name, region) VALUES (%s, %s, %s) "
                             "ON CONFLICT DO NOTHING RETURNING *", (org_id, name, region)).fetchone()

    def update_org(self, org_id: str, name: str | None, region: str | None, status: str | None) -> dict | None:
        with self.pool.connection() as c, c.transaction():
            row = c.execute(
                """UPDATE organisations SET name = coalesce(%s, name), region = coalesce(%s, region),
                       status = coalesce(%s, status), updated_at = now() WHERE org_id = %s RETURNING *""",
                (name, region, status, org_id)).fetchone()
            if row and status == "SUSPENDED":   # sign everyone in it out now
                c.execute("DELETE FROM sessions WHERE username IN (SELECT username FROM users WHERE org_id = %s)",
                          (org_id,))
            return row

    def find_api_key(self, key_sha256: str) -> dict | None:
        with self.pool.connection() as c:
            return c.execute(
                """UPDATE api_keys k SET last_used_at = now() FROM organisations o
                   WHERE k.key_sha256 = %s AND k.revoked_at IS NULL AND o.org_id = k.org_id
                   RETURNING k.org_id, k.prefix, o.status AS org_status""", (key_sha256,)).fetchone()

    def create_api_key(self, key_sha256: str, org_id: str, name: str, prefix: str, created_by: str) -> dict:
        with self.pool.connection() as c:
            return c.execute(
                """INSERT INTO api_keys (key_sha256, org_id, name, prefix, created_by) VALUES (%s,%s,%s,%s,%s)
                   RETURNING left(key_sha256, 12) AS key_id, org_id, name, prefix, created_by, created_at""",
                (key_sha256, org_id, name, prefix, created_by)).fetchone()

    def list_api_keys(self, org: str | None) -> list[dict]:
        with self.pool.connection() as c:
            return c.execute(
                """SELECT left(key_sha256, 12) AS key_id, org_id, name, prefix, created_by, created_at,
                          last_used_at, revoked_at FROM api_keys
                   WHERE %s::text IS NULL OR org_id = %s ORDER BY created_at DESC""", (org, org)).fetchall()

    def revoke_api_key(self, key_id: str, org: str | None) -> bool:
        with self.pool.connection() as c:
            cur = c.execute(
                """UPDATE api_keys SET revoked_at = now() WHERE left(key_sha256, 12) = %s AND revoked_at IS NULL
                   AND (%s::text IS NULL OR org_id = %s)""", (key_id, org, org))
            return cur.rowcount == 1

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
                """INSERT INTO predictions (org_id, transaction_id, source, account_id, counterparty_id, txn_type,
                       amount, region, event_time, ingested_at, scored_at, risk_score, risk_level, decision,
                       reason_codes, model_version, features, feature_ms, inference_ms, pipeline_ms)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           extract(epoch FROM clock_timestamp() - %s::timestamptz) * 1000)
                   ON CONFLICT (org_id, transaction_id) DO NOTHING
                   RETURNING persisted_at, pipeline_ms""",
                (_org(event), event.transaction_id, source.value, event.account_id, event.counterparty_id,
                 event.txn_type.value, event.amount, event.region, event.event_time, ingested_at,
                 score.scored_at, score.risk_score, score.risk_level.value, score.decision.value,
                 score.reason_codes, score.model_version, json.dumps(_json_safe(features)), feature_ms,
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
        org = _org(event)
        c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{org}|{event.account_id}",))
        existing = c.execute(
            """SELECT alert_id, priority, event_count FROM alerts
               WHERE org_id = %s AND account_id = %s AND status IN ('OPEN', 'IN_REVIEW')
                 AND last_event_at > clock_timestamp() - make_interval(mins => %s)
               ORDER BY created_at DESC LIMIT 1 FOR UPDATE""",
            (org, event.account_id, ap.dedup_minutes)).fetchone()
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
                       AND al.org_id = %s
                   GROUP BY a.name, a.ord ORDER BY count(al.alert_id), a.ord LIMIT 1""",
                (ap.analysts, org)).fetchone()
            alert = c.execute(
                """INSERT INTO alerts (org_id, account_id, first_transaction, priority, trigger_types, reason_codes,
                       max_risk_score, total_amount, assigned_to, sla_due_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, clock_timestamp() + make_interval(secs => %s))
                   RETURNING *""",
                (org, event.account_id, event.transaction_id, priority, triggers, score.reason_codes,
                 score.risk_score, event.amount, assignee["name"] if assignee else None,
                 60 * ap.sla_minutes.get(str(priority), 60))).fetchone()
            kind = "CREATED"
        c.execute("INSERT INTO alert_transactions (org_id, transaction_id, alert_id, trigger_types) "
                  "VALUES (%s,%s,%s,%s)", (org, event.transaction_id, alert["alert_id"], triggers))
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
            # One statement: the update and its audit rows travel together, so a
            # backlog of hundreds of overdue alerts costs one round trip, not one each.
            rows = c.execute(
                """WITH up AS (
                       UPDATE alerts SET escalation_level = escalation_level + 1, escalated_at = clock_timestamp(),
                           priority = greatest(priority - 1, 1), assigned_to = %s,
                           sla_due_at = clock_timestamp() + make_interval(secs => 60 * coalesce(
                               (%s::jsonb ->> greatest(priority - 1, 1)::text)::float, 60)),
                           updated_at = clock_timestamp()
                       WHERE status = 'OPEN' AND sla_due_at < clock_timestamp() AND escalation_level < %s
                       RETURNING *),
                   logged AS (
                       INSERT INTO alert_events (alert_id, kind, detail, actor)
                       SELECT alert_id, 'ESCALATED',
                              jsonb_build_object('level', escalation_level, 'assigned_to', %s::text), 'alert-engine'
                       FROM up)
                   SELECT * FROM up""",
                (ap.supervisor, json.dumps(ap.sla_minutes), ap.max_escalations, ap.supervisor)).fetchall()
        return rows

    def save_label(self, transaction_id: str, is_fraud: bool, source: str, org_id: str = "demo") -> None:
        with self.pool.connection() as c:
            c.execute(
                """INSERT INTO labels (org_id, transaction_id, is_fraud, source) VALUES (%s,%s,%s,%s)
                   ON CONFLICT (org_id, transaction_id) DO UPDATE
                   SET is_fraud = EXCLUDED.is_fraud, source = EXCLUDED.source, labelled_at = now()
                   WHERE labels.source <> 'ANALYST' OR EXCLUDED.source = 'ANALYST'""",
                (org_id, transaction_id, is_fraud, source))

    # ---- case management --------------------------------------------------------
    def list_alerts(self, status: str | None, limit: int, org: str | None = None) -> list[dict]:
        with self.pool.connection() as c:
            q = """SELECT a.*, p.source, p.txn_type, p.amount AS first_amount,
                          a.sla_due_at < clock_timestamp() AND a.status = 'OPEN' AS sla_breached
                   FROM alerts a JOIN predictions p ON p.org_id = a.org_id AND p.transaction_id = a.first_transaction
                   WHERE TRUE"""
            params: list = []
            if org:
                q += " AND a.org_id = %s"
                params.append(org)
            if status:
                q += " AND a.status = %s"
                params.append(status)
            q += " ORDER BY a.priority, a.created_at DESC LIMIT %s"
            params.append(limit)
            return c.execute(q, params).fetchall()

    def get_alert(self, alert_id: int, org: str | None = None) -> dict | None:
        with self.pool.connection() as c:
            alert = c.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,)).fetchone()
            if alert is None or (org and alert["org_id"] != org):
                return None
            alert["transactions"] = c.execute(
                """SELECT p.transaction_id, p.source, p.txn_type, p.amount, p.counterparty_id, p.event_time,
                          p.risk_score, p.risk_level, p.reason_codes, t.trigger_types
                   FROM alert_transactions t JOIN predictions p USING (org_id, transaction_id)
                   WHERE t.alert_id = %s ORDER BY p.persisted_at DESC LIMIT 100""", (alert_id,)).fetchall()
            alert["customer_reports"] = c.execute(
                """SELECT report_id, transaction_id, kind, note, username, created_at FROM customer_reports
                   WHERE alert_id = %s ORDER BY report_id DESC""", (alert_id,)).fetchall()
            alert["events"] = c.execute(
                "SELECT kind, detail, actor, at FROM alert_events WHERE alert_id = %s ORDER BY id DESC LIMIT 100",
                (alert_id,)).fetchall()
            return alert

    def update_alert(self, alert_id: int, to_status: str | None, assign_to: str | None, actor: str,
                     notes: str | None, org: str | None = None) -> dict:
        with self.pool.connection() as c, c.transaction():
            cur = c.execute("SELECT * FROM alerts WHERE alert_id = %s FOR UPDATE", (alert_id,)).fetchone()
            if cur is None or (org and cur["org_id"] != org):
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
                    """INSERT INTO labels (org_id, transaction_id, is_fraud, source)
                       SELECT org_id, transaction_id, %s, 'ANALYST' FROM alert_transactions WHERE alert_id = %s
                       ON CONFLICT (org_id, transaction_id) DO UPDATE SET is_fraud = EXCLUDED.is_fraud,
                           source = 'ANALYST', labelled_at = now()""",
                    (to_status == "CONFIRMED_FRAUD", alert_id))
        return row

    def get_transaction(self, transaction_id: str, org: str | None = None) -> dict | None:
        """One transaction. Without `org` (superadmin) the id may exist in several organisations:
        the most recent is returned."""
        with self.pool.connection() as c:
            return c.execute(
                """SELECT p.*, t.alert_id, a.status AS alert_status, a.priority AS alert_priority,
                          l.is_fraud AS label, l.source AS label_source
                   FROM predictions p
                   LEFT JOIN alert_transactions t ON t.org_id = p.org_id AND t.transaction_id = p.transaction_id
                   LEFT JOIN alerts a ON a.alert_id = t.alert_id
                   LEFT JOIN labels l ON l.org_id = p.org_id AND l.transaction_id = p.transaction_id
                   WHERE p.transaction_id = %s AND (%s::text IS NULL OR p.org_id = %s)
                   ORDER BY p.persisted_at DESC LIMIT 1""", (transaction_id, org, org)).fetchone()

    FEED_COLUMNS = """org_id, transaction_id, source, account_id, counterparty_id, txn_type, amount, region, event_time,
                      ingested_at, scored_at, persisted_at, risk_score, risk_level, decision, reason_codes,
                      model_version, inference_ms, pipeline_ms"""

    def recent_transactions(self, limit: int, suspicious_only: bool = False, source: str | None = None,
                            org: str | None = None) -> list[dict]:
        where, params = [], []
        if org:
            where.append("org_id = %s")
            params.append(org)
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
    def stats(self, source: str | None = None, org: str | None = None) -> dict:
        conds, params = [], []
        if source:
            conds.append("source = %s")
            params.append(source)
        if org:
            conds.append("org_id = %s")
            params.append(org)
        f = ("WHERE " + " AND ".join(conds)) if conds else ""
        and_f = ("AND " + " AND ".join(conds)) if conds else ""
        org_f, org_p = ("WHERE org_id = %s", [org]) if org else ("", [])
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
                f"""SELECT source, count(*) AS txns, coalesce(sum(amount), 0)::float AS volume,
                          count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS high_risk,
                          max(persisted_at) AS last_event_at,
                          count(*) FILTER (WHERE persisted_at > clock_timestamp() - interval '60 seconds')
                            AS last_minute
                   FROM predictions {org_f} GROUP BY source ORDER BY txns DESC""",
                org_p).fetchall()
            alerts = c.execute(
                f"""SELECT count(*) AS total,
                          count(*) FILTER (WHERE status = 'OPEN') AS open,
                          count(*) FILTER (WHERE status = 'IN_REVIEW') AS in_review,
                          count(*) FILTER (WHERE status = 'CONFIRMED_FRAUD') AS confirmed,
                          count(*) FILTER (WHERE status = 'FALSE_POSITIVE') AS false_positive,
                          count(*) FILTER (WHERE escalation_level > 0 AND status IN ('OPEN','IN_REVIEW')) AS escalated,
                          count(*) FILTER (WHERE status = 'OPEN' AND sla_due_at < clock_timestamp()) AS sla_breached
                   FROM alerts {org_f}""", org_p).fetchone()
            quality = c.execute(
                f"""SELECT count(*) AS labelled,
                           count(*) FILTER (WHERE l.is_fraud AND p.risk_level IN ('HIGH','CRITICAL')) AS tp,
                           count(*) FILTER (WHERE NOT l.is_fraud AND p.risk_level IN ('HIGH','CRITICAL')) AS fp,
                           count(*) FILTER (WHERE l.is_fraud AND p.risk_level NOT IN ('HIGH','CRITICAL')) AS fn
                    FROM labels l JOIN predictions p USING (org_id, transaction_id)
                        {f.replace("source", "p.source")}""", params).fetchone()
        return {
            "source_filter": source,
            "org_filter": org,
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

    def performance(self, window_minutes: int = 5, org: str | None = None) -> dict:
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
                    WHERE persisted_at > clock_timestamp() - make_interval(mins => %s) AND {LIVE_SOURCES_SQL}
                      AND (%s::text IS NULL OR org_id = %s)""",
                (window_minutes, org, org)).fetchone()

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

    def recent_features(self, hours: int = 24, limit: int = 20_000) -> tuple[list[dict], float]:
        """Feature values of the most recent live predictions, and the hours they span."""
        with self.pool.connection() as c:
            rows = c.execute(
                f"""SELECT features, persisted_at FROM predictions
                    WHERE persisted_at > clock_timestamp() - make_interval(hours => %s) AND {LIVE_SOURCES_SQL}
                    ORDER BY persisted_at DESC LIMIT %s""", (hours, limit)).fetchall()
        if not rows:
            return [], 0.0
        span = (rows[0]["persisted_at"] - rows[-1]["persisted_at"]).total_seconds() / 3600
        return [r["features"] for r in rows], span

    # ---- account holders (CUSTOMER) --------------------------------------------------
    def customer_transactions(self, org: str, account_ids: list[str], limit: int) -> list[dict]:
        """A customer's own payments with a plain status. No scores or reason codes: those are for the bank."""
        if not account_ids:
            return []
        with self.pool.connection() as c:
            return c.execute(
                """SELECT p.transaction_id, p.event_time, p.account_id, p.counterparty_id, p.txn_type, p.amount,
                          CASE WHEN a.status = 'CONFIRMED_FRAUD' THEN 'CONFIRMED_FRAUD'
                               WHEN a.status IN ('OPEN', 'IN_REVIEW') OR p.decision IN ('REVIEW', 'HOLD')
                                    AND coalesce(a.status, '') <> 'FALSE_POSITIVE' THEN 'UNDER_REVIEW'
                               ELSE 'OK' END AS status,
                          r.report_id IS NOT NULL AS reported
                   FROM predictions p
                   LEFT JOIN alert_transactions t USING (org_id, transaction_id)
                   LEFT JOIN alerts a ON a.alert_id = t.alert_id
                   LEFT JOIN LATERAL (SELECT report_id FROM customer_reports cr
                                      WHERE cr.org_id = p.org_id AND cr.transaction_id = p.transaction_id
                                      LIMIT 1) r ON TRUE
                   WHERE p.org_id = %s AND p.account_id = ANY(%s)
                   ORDER BY p.event_time DESC LIMIT %s""", (org, list(account_ids), limit)).fetchall()

    def customer_report(self, org: str, username: str, account_ids: list[str], transaction_id: str,
                        kind: str, note: str | None) -> dict | None:
        """Record a customer's report and put it in front of analysts: it joins the payment's alert,
        or opens a new priority-2 alert. None if the payment is not one of theirs."""
        with self.pool.connection() as c, c.transaction():
            p = c.execute("SELECT * FROM predictions WHERE org_id = %s AND transaction_id = %s "
                          "AND account_id = ANY(%s)", (org, transaction_id, list(account_ids))).fetchone()
            if p is None:
                return None
            c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{org}|{p['account_id']}",))
            existing = c.execute("SELECT alert_id FROM alert_transactions WHERE org_id = %s AND transaction_id = %s",
                                 (org, transaction_id)).fetchone()
            reason = "REPORTED_BY_CUSTOMER"
            if existing:
                alert_id = existing["alert_id"]
                c.execute(
                    """UPDATE alerts SET priority = least(priority, 2),
                           trigger_types = ARRAY(SELECT DISTINCT unnest(trigger_types || '{CUSTOMER_REPORT}')),
                           reason_codes = ARRAY(SELECT DISTINCT unnest(reason_codes || %s::text[])),
                           status = CASE WHEN status = 'FALSE_POSITIVE' THEN 'OPEN' ELSE status END,
                           updated_at = clock_timestamp() WHERE alert_id = %s""", ([reason], alert_id))
            else:
                alert_id = c.execute(
                    """INSERT INTO alerts (org_id, account_id, first_transaction, priority, trigger_types, reason_codes,
                           max_risk_score, total_amount, sla_due_at)
                       VALUES (%s,%s,%s,2,'{CUSTOMER_REPORT}',%s,%s,%s, clock_timestamp() + interval '30 minutes')
                       RETURNING alert_id""",
                    (org, p["account_id"], transaction_id, [reason], p["risk_score"],
                     p["amount"])).fetchone()["alert_id"]
                c.execute("INSERT INTO alert_transactions (org_id, transaction_id, alert_id, trigger_types) "
                          "VALUES (%s,%s,%s,'{CUSTOMER_REPORT}')", (org, transaction_id, alert_id))
            c.execute("INSERT INTO alert_events (alert_id, kind, detail, actor) VALUES (%s,'CUSTOMER_REPORT',%s,%s)",
                      (alert_id, Jsonb({"transaction_id": transaction_id, "kind": kind}), username))
            row = c.execute(
                """INSERT INTO customer_reports (org_id, username, transaction_id, kind, note, alert_id)
                   VALUES (%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (org_id, transaction_id, username) DO UPDATE SET kind = EXCLUDED.kind,
                       note = coalesce(EXCLUDED.note, customer_reports.note)
                   RETURNING report_id, transaction_id, kind, note, alert_id, created_at""",
                (org, username, transaction_id, kind, note, alert_id)).fetchone()
            row["alert"] = c.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,)).fetchone()
            return row

    def customer_reports(self, org: str, username: str) -> list[dict]:
        with self.pool.connection() as c:
            return c.execute(
                """SELECT r.report_id, r.transaction_id, r.kind, r.note, r.created_at,
                          CASE a.status WHEN 'CONFIRMED_FRAUD' THEN 'CONFIRMED_FRAUD'
                                        WHEN 'FALSE_POSITIVE' THEN 'CLOSED_NO_FRAUD'
                                        ELSE 'UNDER_REVIEW' END AS status
                   FROM customer_reports r LEFT JOIN alerts a USING (alert_id)
                   WHERE r.org_id = %s AND r.username = %s ORDER BY r.report_id DESC""", (org, username)).fetchall()

    # ---- public figures for the home page ---------------------------------------------
    def public_summary(self) -> dict:
        """Platform-wide totals only: no organisation, account or amount is identifiable."""
        with self.pool.connection() as c:
            return c.execute(
                """SELECT count(*) AS transactions_24h,
                          count(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS flagged_24h,
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY pipeline_ms)
                              FILTER (WHERE source <> 'HISTORICAL_IMPORT') AS p50_pipeline_ms,
                          (SELECT count(*) FROM organisations WHERE status = 'ACTIVE') AS organisations
                   FROM predictions WHERE persisted_at > now() - interval '24 hours'""").fetchone()



def connect_with_retry(dsn: str, attempts: int = 30) -> Repository:
    for i in range(attempts):
        try:
            repo = Repository(dsn)
            repo.ping()
            return repo
        except psycopg.OperationalError:
            time.sleep(min(2 ** i, 5))
    raise RuntimeError("database not reachable")

