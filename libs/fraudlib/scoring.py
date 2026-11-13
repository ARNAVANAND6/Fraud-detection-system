"""Turns features into a risk score, level, decision and reason codes."""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from .features import FEATURE_NAMES
from .policy import Thresholds
from .rules import RULE_CATEGORY, evaluate_rules
from .schemas import RiskScore, TransactionEvent

# A model feature that pushes the score up becomes a reason code only when the
# plain-language statement is actually true for this transaction. (The first
# live run showed HIGH_VELOCITY_1H on an account with no history: the model
# was reacting to the *absence* of history, and the old mapping mislabelled it.)
FEATURE_REASON = {
    "amount": ("AMOUNT_PATTERN", lambda f: True),
    "log_amount": ("AMOUNT_PATTERN", lambda f: True),
    "amount_vs_avg_24h": ("UNUSUAL_AMOUNT", lambda f: f["amount_vs_avg_24h"] >= 2),
    "amount_to_balance": ("LARGE_SHARE_OF_BALANCE", lambda f: f["amount_to_balance"] >= 0.5),
    "drains_balance": ("BALANCE_DRAIN", lambda f: f["drains_balance"] == 1),
    "count_60s": ("RAPID_TRANSACTION_PATTERN", lambda f: f["count_60s"] >= 1),
    "seconds_since_last": ("RAPID_TRANSACTION_PATTERN", lambda f: 0 <= f["seconds_since_last"] < 60),
    "count_1h": ("HIGH_VELOCITY_1H", lambda f: f["count_1h"] >= 5),
    "sum_1h": ("HIGH_VELOCITY_1H", lambda f: f["count_1h"] >= 5),
    "count_24h": ("LIMITED_ACCOUNT_HISTORY", lambda f: f["count_24h"] == 0),
    "avg_amount_24h": ("LIMITED_ACCOUNT_HISTORY", lambda f: f["count_24h"] == 0),
    "is_new_counterparty": ("NEW_COUNTERPARTY", lambda f: f["is_new_counterparty"] == 1),
    "is_new_device": ("NEW_DEVICE", lambda f: f["is_new_device"] == 1),
    "dest_balance_zero": ("EMPTY_DESTINATION_ACCOUNT", lambda f: f["dest_balance_zero"] == 1),
    "dest_senders_1h": ("SUSPICIOUS_RELATIONSHIP", lambda f: f["dest_senders_1h"] >= 2),
    "is_transfer": ("HIGH_RISK_TXN_TYPE", lambda f: f["is_transfer"] == 1),
    "is_cash_out": ("HIGH_RISK_TXN_TYPE", lambda f: f["is_cash_out"] == 1),
}
MAX_MODEL_REASONS = 3


class Scorer:
    """Loads a LightGBM model if present; otherwise scores with rules only.

    The fallback is visible in `model_version` ("rules-only") so nobody
    mistakes rule scores for model scores.
    """

    def __init__(self, model_dir: str | Path | None):
        self.booster = None
        self.model_version = "rules-only"
        if model_dir:
            meta_path = Path(model_dir) / "model_meta.json"
            if meta_path.exists():
                import lightgbm as lgb

                meta = json.loads(meta_path.read_text())
                if meta["feature_names"] != FEATURE_NAMES:
                    raise ValueError("Model was trained on different features; retrain it (make data train).")
                self.booster = lgb.Booster(model_file=str(Path(model_dir) / meta["model_file"]))
                self.model_version = meta["model_version"]

    def _model_probability(self, features: dict[str, float]) -> float:
        # One row, one thread: LightGBM's thread pool costs far more than it
        # saves on a single row (measured here: about 16 ms with the default
        # threads, about 0.03 ms with one).
        row = np.array([[features[n] for n in FEATURE_NAMES]], dtype=np.float64)
        return float(self.booster.predict(row, num_threads=1)[0])

    def _model_reasons(self, features: dict[str, float]) -> list[str]:
        """TreeSHAP contributions for this transaction, strongest first. This is
        the expensive part (about 3 ms), so it only runs for scores that need
        an explanation."""
        row = np.array([[features[n] for n in FEATURE_NAMES]], dtype=np.float64)
        contrib = self.booster.predict(row, pred_contrib=True, num_threads=1)[0]
        top = sorted(zip(FEATURE_NAMES, contrib[:-1], strict=True), key=lambda kv: kv[1], reverse=True)
        reasons: list[str] = []
        for name, value in top:
            if value <= 0 or len(reasons) == MAX_MODEL_REASONS:
                break
            code, is_true = FEATURE_REASON.get(name, (None, None))
            if code and code not in reasons and is_true(features):
                reasons.append(code)
        return reasons

    def score(self, event: TransactionEvent, features: dict[str, float], thresholds: Thresholds) -> RiskScore:
        t0 = time.perf_counter()
        rule_score, rule_codes = evaluate_rules(features)
        if self.booster is not None:
            risk = self._model_probability(features)
            # A low score needs no model explanation.
            model_codes = self._model_reasons(features) if risk >= thresholds.medium else []
        else:
            risk, model_codes = rule_score, []
        risk = min(1.0, max(0.0, risk))
        level = thresholds.level(risk)
        return RiskScore(
            transaction_id=event.transaction_id,
            risk_score=round(risk, 6),
            risk_level=level,
            decision=thresholds.decision(level),
            reason_codes=list(dict.fromkeys(rule_codes + model_codes)),
            rule_categories=list(dict.fromkeys(RULE_CATEGORY[c] for c in rule_codes)),
            model_version=self.model_version,
            scored_at=datetime.now(UTC),
            latency_ms=round((time.perf_counter() - t0) * 1000, 3),
        )
