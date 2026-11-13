"""Event and API contracts shared by every service.

One definition here is used by the gateway, the generator, the scoring
service and the tests. Spark mirrors these fields in
services/streaming/stream_job.py; a unit test checks the two stay in sync.
"""
from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = 1


class TxnType(str, Enum):
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    CASH_IN = "CASH_IN"
    DEBIT = "DEBIT"


class Source(str, Enum):
    """Where an event entered the platform. Shown on every transaction."""

    LIVE_API = "LIVE_API"                    # authenticated REST ingestion
    LIVE_WEBHOOK = "LIVE_WEBHOOK"            # signed payment-gateway webhook
    KAFKA_STREAM = "KAFKA_STREAM"            # produced straight to Kafka by an integration
    HISTORICAL_IMPORT = "HISTORICAL_IMPORT"  # uploaded CSV / Parquet
    SIMULATED_DEMO = "SIMULATED_DEMO"        # built-in synthetic generator


class TransactionEvent(BaseModel):
    """A payment as the switch sees it at authorisation time.

    It deliberately has no label and no post-transaction balances: those are
    not known when the transaction is scored.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(SCHEMA_VERSION, ge=1, le=SCHEMA_VERSION)
    transaction_id: str = Field(min_length=1, max_length=64)
    event_time: datetime
    account_id: str = Field(min_length=1, max_length=64)
    counterparty_id: str = Field(min_length=1, max_length=64)
    txn_type: TxnType
    amount: float = Field(gt=0, lt=1e9)
    old_balance_orig: float = Field(ge=0)
    old_balance_dest: float = Field(ge=0)
    channel: str = Field(default="APP", max_length=16)
    region: str = Field(default="UNKNOWN", max_length=32)  # synthetic, not real geography
    device_id: str | None = Field(default=None, max_length=64)
    # Set by the ingestion point, never trusted from an outside caller.
    source: Source | None = None
    ingested_at: datetime | None = None

    @field_validator("event_time", "ingested_at")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return v
        if v.tzinfo is None:
            raise ValueError("timestamps must include a timezone")
        return v.astimezone(UTC)

    @property
    def event_ms(self) -> int:
        return int(self.event_time.timestamp() * 1000)


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Decision(str, Enum):
    """A recommendation for a human or a downstream policy engine.

    It is not a banking or regulatory decision.
    """

    ALLOW = "ALLOW"
    REVIEW = "REVIEW"
    HOLD = "HOLD"


class RiskScore(BaseModel):
    transaction_id: str
    risk_score: float = Field(ge=0, le=1)  # model score, not a calibrated probability
    risk_level: RiskLevel
    decision: Decision
    reason_codes: list[str]
    rule_categories: list[str] = []
    model_version: str
    scored_at: datetime                    # inference timestamp
    latency_ms: float | None = None        # rules + model inference only
    disclaimer: str = "Model risk estimate for review; not a banking decision."
