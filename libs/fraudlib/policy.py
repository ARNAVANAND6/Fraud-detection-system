"""Runtime-configurable policy: risk thresholds and alert-engine settings.

Stored as one JSON document in PostgreSQL (table `settings`, key `policy`),
editable through the API, and reloaded by the scorer every few seconds.
"""
from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from .schemas import Decision, RiskLevel

LEVEL_ORDER = [RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL]


class Thresholds(BaseModel):
    medium: float = Field(0.30, gt=0, lt=1)
    high: float = Field(0.60, gt=0, lt=1)
    critical: float = Field(0.90, gt=0, le=1)

    @model_validator(mode="after")
    def _ordered(self):
        if not self.medium < self.high < self.critical:
            raise ValueError("thresholds must satisfy medium < high < critical")
        return self

    def level(self, score: float) -> RiskLevel:
        if score >= self.critical:
            return RiskLevel.CRITICAL
        if score >= self.high:
            return RiskLevel.HIGH
        if score >= self.medium:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    @staticmethod
    def decision(level: RiskLevel) -> Decision:
        return {RiskLevel.CRITICAL: Decision.HOLD, RiskLevel.HIGH: Decision.REVIEW}.get(level, Decision.ALLOW)


class AlertPolicy(BaseModel):
    # A score at or above this level always raises an alert.
    score_alert_level: RiskLevel = RiskLevel.HIGH
    # Rule categories that can raise an alert by themselves, and the minimum
    # risk level the transaction needs for that to happen.
    rule_triggers: list[str] = ["FREQUENCY", "AMOUNT", "BEHAVIOUR", "RELATIONSHIP"]
    rule_min_level: RiskLevel = RiskLevel.MEDIUM
    # Further suspicious events on an account with an open alert are attached
    # to that alert instead of opening a new one.
    dedup_minutes: int = Field(15, ge=0, le=1440)
    # Minutes an OPEN (unacknowledged) alert may wait before it is escalated.
    sla_minutes: dict[str, float] = {"1": 5, "2": 15, "3": 60, "4": 240}
    max_escalations: int = Field(2, ge=0, le=5)
    analysts: list[str] = ["analyst-1", "analyst-2", "analyst-3"]
    supervisor: str = "supervisor"


class Policy(BaseModel):
    thresholds: Thresholds = Thresholds()
    alerts: AlertPolicy = AlertPolicy()


def alert_triggers(policy: Policy, level: RiskLevel, rule_categories: list[str]) -> list[str]:
    """Which alert triggers fire for a scored transaction (empty = no alert)."""
    rank = LEVEL_ORDER.index
    triggers = []
    if rank(level) >= rank(policy.alerts.score_alert_level):
        triggers.append("RISK_SCORE")
    if rank(level) >= rank(policy.alerts.rule_min_level):
        triggers += [c for c in dict.fromkeys(rule_categories) if c in policy.alerts.rule_triggers]
    return triggers


def alert_priority(level: RiskLevel, triggers: list[str]) -> int:
    """1 is most urgent."""
    if level == RiskLevel.CRITICAL:
        return 1
    if level == RiskLevel.HIGH:
        return 2
    return 3 if len(triggers) >= 2 else 4
