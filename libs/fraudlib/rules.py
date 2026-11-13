"""Deterministic rules. They give reason codes that do not depend on the model,
cover accounts with no history, and are the baseline the model must beat.

Each rule belongs to a category the alert engine can trigger on:
FREQUENCY, AMOUNT, BEHAVIOUR or RELATIONSHIP.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

MULE_FAN_IN_SENDERS = 20  # distinct senders to one destination within an hour


@dataclass(frozen=True)
class Rule:
    code: str
    category: str
    weight: float
    check: Callable[[dict[str, float]], bool]
    description: str


def _moves_money_out(f: dict[str, float]) -> bool:
    return f["is_transfer"] == 1 or f["is_cash_out"] == 1


RULES: list[Rule] = [
    Rule("RAPID_TRANSACTION_PATTERN", "FREQUENCY", 0.35,
         lambda f: f["count_60s"] >= 3,
         "3 or more earlier transactions from this account in the last 60 seconds"),
    Rule("HIGH_VELOCITY_1H", "FREQUENCY", 0.2,
         lambda f: f["count_1h"] >= 10,
         "10 or more earlier transactions in the last hour"),
    Rule("UNUSUAL_AMOUNT", "AMOUNT", 0.3,
         lambda f: f["count_24h"] >= 3 and f["amount_vs_avg_24h"] >= 5,
         "Amount is at least 5x the account's 24h average"),
    Rule("BALANCE_DRAIN", "AMOUNT", 0.35,
         lambda f: f["drains_balance"] == 1 and _moves_money_out(f),
         "Transfer or cash-out of 95% or more of the available balance"),
    Rule("NEW_COUNTERPARTY_HIGH_AMOUNT", "AMOUNT", 0.2,
         lambda f: f["is_new_counterparty"] == 1 and f["amount"] >= 50_000,
         "First payment to this counterparty and amount of 50,000 or more"),
    Rule("NEW_DEVICE", "BEHAVIOUR", 0.15,
         lambda f: f["is_new_device"] == 1 and f["count_24h"] >= 1,
         "Device not seen before for an active account"),
    Rule("BEHAVIOUR_CHANGE", "BEHAVIOUR", 0.3,
         lambda f: f["is_new_device"] == 1 and f["is_new_counterparty"] == 1 and _moves_money_out(f)
         and (f["amount_to_balance"] >= 0.5 or f["amount_vs_avg_24h"] >= 3),
         "New device and new counterparty together with an unusually large outflow"),
    Rule("MULE_FAN_IN", "RELATIONSHIP", 0.35,
         lambda f: f["dest_senders_1h"] >= MULE_FAN_IN_SENDERS and _moves_money_out(f),
         f"Destination received money from {MULE_FAN_IN_SENDERS}+ different accounts in the last hour"),
]
RULE_CATEGORY = {r.code: r.category for r in RULES}


def evaluate_rules(features: dict[str, float]) -> tuple[float, list[str]]:
    """Return (rule score in [0, 1], reason codes of the rules that fired)."""
    hits = [r for r in RULES if r.check(features)]
    score = min(1.0, sum(r.weight for r in hits))
    return score, [r.code for r in hits]
