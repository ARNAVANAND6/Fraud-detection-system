"""Train and compare fraud models with a time-based split.

Input: a CSV with TransactionEvent columns + is_fraud (the generator's csv
mode), or PaySim's CSV with --paysim. Features come from the same
fraudlib.features code the scoring service uses, computed point-in-time.

Split: first 70% of time = train, next 15% = validation (threshold choice),
last 15% = test (touched once, for the final report). No shuffling.

Outputs: models/<version>/ (LightGBM model + metadata), reports/<version>.json.
If MLFLOW_TRACKING_URI is set and mlflow is installed, runs are logged there.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "libs"))
from fraudlib.features import FEATURE_NAMES, OfflineFeatureBuilder
from fraudlib.rules import evaluate_rules
from fraudlib.schemas import TransactionEvent


def load_events(path: Path, paysim: bool, limit: int | None) -> tuple[list[TransactionEvent], np.ndarray]:
    df = pd.read_csv(path, nrows=limit)
    if paysim:
        # PaySim: step = hour index. newbalance* columns are post-transaction
        # values (not known at scoring time) and are deliberately dropped.
        start = datetime(2026, 1, 1, tzinfo=UTC)
        rng = np.random.default_rng(0)
        secs = rng.integers(0, 3600, len(df))
        df = pd.DataFrame({
            "transaction_id": [f"PS_{i}" for i in range(len(df))],
            "event_time": [start + timedelta(hours=int(s), seconds=int(x))
                           for s, x in zip(df["step"], secs, strict=True)],
            "account_id": df["nameOrig"], "counterparty_id": df["nameDest"], "txn_type": df["type"],
            "amount": df["amount"].clip(lower=0.01), "old_balance_orig": df["oldbalanceOrg"],
            "old_balance_dest": df["oldbalanceDest"], "is_fraud": df["isFraud"],
        })
    else:
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    cols = [c for c in TransactionEvent.model_fields if c in df.columns]
    events = []
    for rec in df[cols].to_dict("records"):
        rec = {k: v for k, v in rec.items() if not (isinstance(v, float) and np.isnan(v))}
        rec["event_time"] = rec["event_time"].to_pydatetime() if hasattr(rec["event_time"], "to_pydatetime") \
            else rec["event_time"]
        events.append(TransactionEvent(**rec))
    return events, df["is_fraud"].to_numpy().astype(int)


def time_split(times: np.ndarray, train: float = 0.7, val: float = 0.15):
    t1, t2 = np.quantile(times, [train, train + val])
    return times < t1, (times >= t1) & (times < t2), times >= t2


def threshold_for_capacity(scores: np.ndarray, alert_rate: float) -> float:
    """Threshold that flags `alert_rate` of traffic (the analysts' review capacity)."""
    return float(np.quantile(scores, 1 - alert_rate))


def evaluate(y: np.ndarray, p: np.ndarray, threshold: float) -> dict:
    pred = (p >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return {
        "pr_auc": float(average_precision_score(y, p)),
        "roc_auc": float(roc_auc_score(y, p)),
        "threshold": threshold,
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "false_positive_rate": float(fp / (fp + tn)) if fp + tn else 0.0,
        "alerts": int(pred.sum()),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=Path("data/synthetic/transactions.csv"))
    ap.add_argument("--paysim", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--alert-rate", type=float, default=0.01, help="share of traffic analysts can review")
    ap.add_argument("--version", default=datetime.now(UTC).strftime("v%Y%m%d%H%M"))
    a = ap.parse_args()

    events, y = load_events(a.data, a.paysim, a.limit)
    X = pd.DataFrame(OfflineFeatureBuilder().build(events))[FEATURE_NAMES]
    times = np.array([e.event_ms for e in events])
    tr, va, te = time_split(times)
    print(f"rows={len(y)} fraud_rate={y.mean():.4%} train={tr.sum()} val={va.sum()} test={te.sum()}")
    print(f"fraud in train/val/test: {y[tr].sum()}/{y[va].sum()}/{y[te].sum()}")

    pos_weight = (len(y[tr]) - y[tr].sum()) / max(y[tr].sum(), 1)
    import lightgbm as lgb

    models = {
        "rules_baseline": None,
        "logistic_regression": make_pipeline(StandardScaler(), LogisticRegression(
            max_iter=2000, class_weight="balanced")),
        "random_forest": RandomForestClassifier(n_estimators=300, min_samples_leaf=5, n_jobs=-1,
                                                class_weight="balanced_subsample", random_state=0),
        "lightgbm": lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31,
                                       min_child_samples=50, subsample=0.8, subsample_freq=1,
                                       colsample_bytree=0.8, scale_pos_weight=pos_weight,
                                       random_state=0, verbose=-1),
    }
    # XGBoost slots in the same way (xgboost.XGBClassifier) once installed.
    results, fitted = {}, {}
    for name, model in models.items():
        if model is None:
            score_fn = lambda df: np.array([evaluate_rules(r)[0] for r in df.to_dict("records")])
        else:
            model.fit(X[tr], y[tr])
            fitted[name] = model
            score_fn = lambda df, m=model: m.predict_proba(df)[:, 1]
        p_val, p_test = score_fn(X[va]), score_fn(X[te])
        thr = threshold_for_capacity(p_val, a.alert_rate)  # chosen on validation only
        results[name] = {"validation": evaluate(y[va], p_val, thr), "test": evaluate(y[te], p_test, thr)}
        t = results[name]["test"]
        print(f"{name:22s} test PR-AUC={t['pr_auc']:.4f} ROC-AUC={t['roc_auc']:.4f} "
              f"precision={t['precision']:.3f} recall={t['recall']:.3f} FPR={t['false_positive_rate']:.4f}")

    lgbm = fitted["lightgbm"]
    importance = dict(sorted(zip(FEATURE_NAMES, lgbm.booster_.feature_importance("gain").tolist(), strict=True),
                             key=lambda kv: -kv[1]))
    out_dir = Path("models") / a.version
    out_dir.mkdir(parents=True, exist_ok=True)
    lgbm.booster_.save_model(str(out_dir / "model.txt"))
    meta = {
        "model_version": a.version, "model_file": "model.txt", "feature_names": FEATURE_NAMES,
        "threshold_review": results["lightgbm"]["validation"]["threshold"],
        "alert_rate": a.alert_rate, "trained_at": datetime.now(UTC).isoformat(),
        "data": str(a.data), "rows": len(y),
    }
    (out_dir / "model_meta.json").write_text(json.dumps(meta, indent=2))
    Path("reports").mkdir(exist_ok=True)
    report = {"meta": meta, "results": results, "lightgbm_gain_importance": importance,
              "caveat": "Synthetic data from this project's own simulator; not real-world performance."}
    (Path("reports") / f"train_{a.version}.json").write_text(json.dumps(report, indent=2))
    print(f"saved {out_dir} and reports/train_{a.version}.json")

    if os.environ.get("MLFLOW_TRACKING_URI"):
        import mlflow

        mlflow.set_experiment("fraud-detection")
        for name, r in results.items():
            with mlflow.start_run(run_name=f"{a.version}-{name}"):
                mlflow.log_params({"model": name, "alert_rate": a.alert_rate, "rows": len(y)})
                mlflow.log_metrics({f"test_{k}": v for k, v in r["test"].items() if isinstance(v, float)})
                if name == "lightgbm":
                    mlflow.log_artifacts(str(out_dir), artifact_path="model")


if __name__ == "__main__":
    main()
