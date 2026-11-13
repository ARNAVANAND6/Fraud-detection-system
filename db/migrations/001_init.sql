-- Operational store: scored transactions, alerts, labels, runtime settings.
-- Every write is keyed on transaction_id so replays are harmless.

CREATE TABLE IF NOT EXISTS predictions (
    transaction_id   TEXT PRIMARY KEY,
    source           TEXT NOT NULL CHECK (source IN
                       ('LIVE_API', 'LIVE_WEBHOOK', 'KAFKA_STREAM', 'HISTORICAL_IMPORT', 'SIMULATED_DEMO')),
    account_id       TEXT NOT NULL,
    counterparty_id  TEXT NOT NULL,
    txn_type         TEXT NOT NULL,
    amount           NUMERIC(14, 2) NOT NULL,
    region           TEXT,
    event_time       TIMESTAMPTZ NOT NULL,          -- when the payment happened
    ingested_at      TIMESTAMPTZ NOT NULL,          -- when the platform received it
    scored_at        TIMESTAMPTZ NOT NULL,          -- model inference timestamp
    persisted_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    risk_score       DOUBLE PRECISION NOT NULL CHECK (risk_score BETWEEN 0 AND 1),
    risk_level       TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    decision         TEXT NOT NULL,
    reason_codes     TEXT[] NOT NULL DEFAULT '{}',
    model_version    TEXT NOT NULL,
    features         JSONB NOT NULL,
    feature_ms       DOUBLE PRECISION,              -- measured: Redis feature read + update
    inference_ms     DOUBLE PRECISION,              -- measured: rules + model
    pipeline_ms      DOUBLE PRECISION               -- measured: ingested_at -> persisted_at
);
CREATE INDEX IF NOT EXISTS predictions_persisted_idx ON predictions (persisted_at DESC);
CREATE INDEX IF NOT EXISTS predictions_account_idx ON predictions (account_id, event_time DESC);
CREATE INDEX IF NOT EXISTS predictions_risky_idx ON predictions (persisted_at DESC)
    WHERE risk_level IN ('HIGH', 'CRITICAL');

CREATE TABLE IF NOT EXISTS alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    account_id        TEXT NOT NULL,
    first_transaction TEXT NOT NULL REFERENCES predictions (transaction_id),
    priority          SMALLINT NOT NULL CHECK (priority BETWEEN 1 AND 4),   -- 1 = most urgent
    status            TEXT NOT NULL DEFAULT 'OPEN'
                      CHECK (status IN ('OPEN', 'IN_REVIEW', 'CONFIRMED_FRAUD', 'FALSE_POSITIVE')),
    trigger_types     TEXT[] NOT NULL DEFAULT '{}',
    reason_codes      TEXT[] NOT NULL DEFAULT '{}',
    max_risk_score    DOUBLE PRECISION NOT NULL,
    event_count       INTEGER NOT NULL DEFAULT 1,
    total_amount      NUMERIC(16, 2) NOT NULL DEFAULT 0,
    assigned_to       TEXT,
    escalation_level  SMALLINT NOT NULL DEFAULT 0,
    escalated_at      TIMESTAMPTZ,
    sla_due_at        TIMESTAMPTZ NOT NULL,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_event_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS alerts_queue_idx ON alerts (status, priority, created_at);
CREATE INDEX IF NOT EXISTS alerts_account_open_idx ON alerts (account_id) WHERE status IN ('OPEN', 'IN_REVIEW');

-- A transaction belongs to at most one alert (duplicate-alert prevention).
CREATE TABLE IF NOT EXISTS alert_transactions (
    transaction_id  TEXT PRIMARY KEY REFERENCES predictions (transaction_id),
    alert_id        BIGINT NOT NULL REFERENCES alerts (alert_id),
    trigger_types   TEXT[] NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS alert_transactions_alert_idx ON alert_transactions (alert_id);

-- Audit trail: creation, attached events, status changes, assignment, escalation.
CREATE TABLE IF NOT EXISTS alert_events (
    id        BIGSERIAL PRIMARY KEY,
    alert_id  BIGINT NOT NULL REFERENCES alerts (alert_id),
    kind      TEXT NOT NULL,
    detail    JSONB NOT NULL DEFAULT '{}',
    actor     TEXT NOT NULL,
    at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Labels arrive later than scores (analyst verdicts, simulator ground truth).
CREATE TABLE IF NOT EXISTS labels (
    transaction_id  TEXT PRIMARY KEY,
    is_fraud        BOOLEAN NOT NULL,
    source          TEXT NOT NULL CHECK (source IN ('ANALYST', 'SIMULATOR')),
    labelled_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_by  TEXT NOT NULL DEFAULT 'system',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
