from datetime import UTC, datetime

import pytest
from fraudlib.features import FEATURE_NAMES, OfflineFeatureBuilder
from fraudlib.policy import AlertPolicy, Policy, Thresholds, alert_priority, alert_triggers
from fraudlib.rules import RULE_CATEGORY, evaluate_rules
from fraudlib.schemas import Decision, RiskLevel, TransactionEvent
from fraudlib.scoring import Scorer
from pydantic import ValidationError


def feats(**over):
    base = dict.fromkeys(FEATURE_NAMES, 0.0)
    base.update(amount=100.0, seconds_since_last=-1.0)
    base.update(over)
    return base


def event(amount=100.0, bal=10_000.0):
    return TransactionEvent(transaction_id="T1", event_time=datetime(2026, 1, 1, tzinfo=UTC), account_id="C1",
                            counterparty_id="C2", txn_type="TRANSFER", amount=amount, old_balance_orig=bal,
                            old_balance_dest=0)


def test_no_rules_fire_on_ordinary_payment():
    assert evaluate_rules(feats()) == (0.0, [])


@pytest.mark.parametrize("over,code,category", [
    ({"count_60s": 3}, "RAPID_TRANSACTION_PATTERN", "FREQUENCY"),
    ({"count_1h": 10}, "HIGH_VELOCITY_1H", "FREQUENCY"),
    ({"count_24h": 3, "amount_vs_avg_24h": 5}, "UNUSUAL_AMOUNT", "AMOUNT"),
    ({"drains_balance": 1, "is_transfer": 1}, "BALANCE_DRAIN", "AMOUNT"),
    ({"is_new_counterparty": 1, "amount": 50_000}, "NEW_COUNTERPARTY_HIGH_AMOUNT", "AMOUNT"),
    ({"is_new_device": 1, "count_24h": 1}, "NEW_DEVICE", "BEHAVIOUR"),
    ({"is_new_device": 1, "is_new_counterparty": 1, "is_cash_out": 1, "amount_to_balance": 0.6},
     "BEHAVIOUR_CHANGE", "BEHAVIOUR"),
    ({"dest_senders_1h": 20, "is_transfer": 1}, "MULE_FAN_IN", "RELATIONSHIP"),
])
def test_each_rule_fires_with_its_category(over, code, category):
    score, codes = evaluate_rules(feats(**over))
    assert code in codes and 0 < score <= 1
    assert RULE_CATEGORY[code] == category


def test_rule_score_is_capped():
    score, _ = evaluate_rules(feats(count_60s=5, count_1h=20, drains_balance=1, is_transfer=1, count_24h=5,
                                    amount_vs_avg_24h=9, is_new_device=1, is_new_counterparty=1,
                                    amount=90_000, dest_senders_1h=30))
    assert score == 1.0


def test_four_risk_tiers_and_decisions():
    t = Thresholds(medium=0.3, high=0.6, critical=0.9)
    assert [t.level(x) for x in (0.0, 0.29, 0.3, 0.59, 0.6, 0.89, 0.9, 1.0)] == [
        RiskLevel.LOW, RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.MEDIUM,
        RiskLevel.HIGH, RiskLevel.HIGH, RiskLevel.CRITICAL, RiskLevel.CRITICAL]
    assert t.decision(RiskLevel.LOW) == t.decision(RiskLevel.MEDIUM) == Decision.ALLOW
    assert t.decision(RiskLevel.HIGH) == Decision.REVIEW
    assert t.decision(RiskLevel.CRITICAL) == Decision.HOLD


def test_thresholds_must_be_ordered():
    with pytest.raises(ValidationError):
        Thresholds(medium=0.7, high=0.6, critical=0.9)


def test_alert_triggers():
    p = Policy()
    assert alert_triggers(p, RiskLevel.LOW, []) == []
    assert alert_triggers(p, RiskLevel.HIGH, []) == ["RISK_SCORE"]
    assert alert_triggers(p, RiskLevel.CRITICAL, ["AMOUNT", "AMOUNT", "FREQUENCY"]) == [
        "RISK_SCORE", "AMOUNT", "FREQUENCY"]
    # Rule categories alone need the configured minimum level.
    lvl = p.alerts.rule_min_level
    assert "RELATIONSHIP" in alert_triggers(p, lvl, ["RELATIONSHIP"])
    off = Policy(alerts=AlertPolicy(rule_triggers=[]))
    assert alert_triggers(off, RiskLevel.MEDIUM, ["AMOUNT"]) == []


def test_alert_priority():
    assert alert_priority(RiskLevel.CRITICAL, ["RISK_SCORE"]) == 1
    assert alert_priority(RiskLevel.HIGH, ["RISK_SCORE"]) == 2
    assert alert_priority(RiskLevel.MEDIUM, ["AMOUNT", "FREQUENCY"]) == 3
    assert alert_priority(RiskLevel.MEDIUM, ["AMOUNT"]) == 4


def test_rules_only_scorer_when_no_model(tmp_path):
    s = Scorer(tmp_path)
    e = event(amount=9_900, bal=10_000)
    r = s.score(e, OfflineFeatureBuilder().build([e])[0], Thresholds())
    assert s.model_version == "rules-only" and r.model_version == "rules-only"
    assert "BALANCE_DRAIN" in r.reason_codes and r.rule_categories == ["AMOUNT"]
    assert r.risk_level == Thresholds().level(r.risk_score)


def test_model_scorer_returns_valid_score():
    s = Scorer("models/current")
    assert s.booster is not None, "run `make data train` first"
    e = event(amount=1000, bal=1000)
    r = s.score(e, OfflineFeatureBuilder().build([e])[0], Thresholds())
    assert 0 <= r.risk_score <= 1 and r.model_version != "rules-only" and r.latency_ms > 0
    # The score reported is the model's own probability.
    row = [[OfflineFeatureBuilder().build([e])[0][n] for n in FEATURE_NAMES]]
    assert r.risk_score == pytest.approx(float(s.booster.predict(row)[0]), abs=1e-6)


def test_scorer_refuses_model_with_other_features(tmp_path):
    (tmp_path / "model_meta.json").write_text('{"feature_names": ["amount"], "model_file": "m", "model_version": "x"}')
    with pytest.raises(ValueError):
        Scorer(tmp_path)


def test_model_reasons_are_true_statements():
    """A reason code is only shown when it actually holds for the transaction."""
    s = Scorer("models/current")
    e = event(amount=9_900, bal=10_000)  # first-ever transaction of the account
    f = OfflineFeatureBuilder().build([e])[0]
    r = s.score(e, f, Thresholds(medium=0.01, high=0.02, critical=0.03))
    assert "HIGH_VELOCITY_1H" not in r.reason_codes and "RAPID_TRANSACTION_PATTERN" not in r.reason_codes
    assert "NEW_DEVICE" not in r.reason_codes  # the event carries no device
