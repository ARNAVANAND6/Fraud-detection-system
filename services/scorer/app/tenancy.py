"""Organisations: several banks (or other payment firms) on one Foresentry.

Every stored transaction, alert and label belongs to one organisation
(`org_id`). People belong to one organisation, except SUPERADMIN, who runs
the platform and sees all of them. A CUSTOMER is an account holder at one
organisation and sees only the accounts linked to them.

Data that existed before organisations belongs to `demo`. Transaction ids
need only be unique within an organisation: the keys are (org_id,
transaction_id).

Applied at start-up by the API and the scorer (under the same advisory lock
as the auth schema), so an existing database is upgraded in place.
"""
from __future__ import annotations

from fastapi import HTTPException

DEFAULT_ORG = "demo"
SUPERADMIN = "SUPERADMIN"
CUSTOMER = "CUSTOMER"

TENANCY_SCHEMA = """
CREATE TABLE IF NOT EXISTS organisations (
    org_id      TEXT PRIMARY KEY CHECK (org_id ~ '^[a-z0-9][a-z0-9-]{1,31}$'),
    name        TEXT NOT NULL,
    region      TEXT NOT NULL DEFAULT 'IN',
    status      TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO organisations (org_id, name) VALUES ('demo', 'Demo Bank') ON CONFLICT DO NOTHING;

ALTER TABLE users ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organisations (org_id);
ALTER TABLE users ADD COLUMN IF NOT EXISTS account_ids TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check
    CHECK (role IN ('VIEWER', 'ANALYST', 'ADMIN', 'SUPERADMIN', 'CUSTOMER'));
UPDATE users SET org_id = 'demo' WHERE org_id IS NULL AND role <> 'SUPERADMIN';
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_org_check;
ALTER TABLE users ADD CONSTRAINT users_org_check CHECK ((role = 'SUPERADMIN') = (org_id IS NULL));
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS org_id TEXT;

ALTER TABLE predictions ADD COLUMN IF NOT EXISTS org_id TEXT NOT NULL DEFAULT 'demo';
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS org_id TEXT NOT NULL DEFAULT 'demo';
ALTER TABLE alert_transactions ADD COLUMN IF NOT EXISTS org_id TEXT NOT NULL DEFAULT 'demo';
ALTER TABLE labels ADD COLUMN IF NOT EXISTS org_id TEXT NOT NULL DEFAULT 'demo';

-- Keys become (org_id, transaction_id). Done once; later starts skip it.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'predictions_org_pkey') THEN
        ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_first_transaction_fkey;
        ALTER TABLE alert_transactions DROP CONSTRAINT IF EXISTS alert_transactions_transaction_id_fkey;
        ALTER TABLE alert_transactions DROP CONSTRAINT IF EXISTS alert_transactions_pkey;
        ALTER TABLE labels DROP CONSTRAINT IF EXISTS labels_pkey;
        ALTER TABLE predictions DROP CONSTRAINT IF EXISTS predictions_pkey;
        ALTER TABLE predictions ADD CONSTRAINT predictions_org_pkey PRIMARY KEY (org_id, transaction_id);
        ALTER TABLE alert_transactions ADD CONSTRAINT alert_transactions_org_pkey
            PRIMARY KEY (org_id, transaction_id);
        ALTER TABLE labels ADD CONSTRAINT labels_org_pkey PRIMARY KEY (org_id, transaction_id);
        ALTER TABLE alerts ADD CONSTRAINT alerts_first_transaction_org_fkey
            FOREIGN KEY (org_id, first_transaction) REFERENCES predictions (org_id, transaction_id);
        ALTER TABLE alert_transactions ADD CONSTRAINT alert_transactions_org_fkey
            FOREIGN KEY (org_id, transaction_id) REFERENCES predictions (org_id, transaction_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS predictions_org_persisted_idx ON predictions (org_id, persisted_at DESC);
CREATE INDEX IF NOT EXISTS predictions_org_account_idx ON predictions (org_id, account_id, event_time DESC);
CREATE INDEX IF NOT EXISTS alerts_org_queue_idx ON alerts (org_id, status, priority, created_at);
CREATE INDEX IF NOT EXISTS alerts_org_account_open_idx ON alerts (org_id, account_id)
    WHERE status IN ('OPEN', 'IN_REVIEW');

-- DISMISSED: closed by an admin without anyone reviewing it (a stale backlog). It is not a verdict and
-- writes no label, so it never counts as fraud or as a false positive.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'alerts'::regclass AND contype = 'c'
                   AND pg_get_constraintdef(oid) LIKE '%DISMISSED%') THEN
        ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_status_check;
        ALTER TABLE alerts ADD CONSTRAINT alerts_status_check
            CHECK (status IN ('OPEN', 'IN_REVIEW', 'CONFIRMED_FRAUD', 'FALSE_POSITIVE', 'DISMISSED'));
    END IF;
END $$;

-- Machine keys issued by an organisation's administrator. Only digests are stored.
CREATE TABLE IF NOT EXISTS api_keys (
    key_sha256    TEXT PRIMARY KEY,
    org_id        TEXT NOT NULL REFERENCES organisations (org_id),
    name          TEXT NOT NULL,
    prefix        TEXT NOT NULL,
    created_by    TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);

-- "This wasn't me" and scam reports from account holders. Each becomes or joins an alert.
CREATE TABLE IF NOT EXISTS customer_reports (
    report_id       BIGSERIAL PRIMARY KEY,
    org_id          TEXT NOT NULL,
    username        TEXT NOT NULL REFERENCES users (username) ON DELETE CASCADE,
    transaction_id  TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('NOT_ME', 'SCAM', 'OTHER')),
    note            TEXT,
    alert_id        BIGINT REFERENCES alerts (alert_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, transaction_id, username)
);
"""


def scope(who, requested: str | None = None) -> str | None:
    """The organisation a request may see. None means every organisation (SUPERADMIN only)."""
    if who.role == SUPERADMIN:
        return requested or None
    if requested and requested != who.org_id:
        raise HTTPException(status_code=403, detail="you can only see your own organisation")
    return who.org_id or DEFAULT_ORG


def ns(org_id: str | None, key: str) -> str:
    """Redis key namespace. The demo organisation keeps the old, un-prefixed keys so its history survives."""
    return key if not org_id or org_id == DEFAULT_ORG else f"{org_id}|{key}"
