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
import time
from datetime import UTC, datetime
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
from fraudlib.drift import build_reference
from fraudlib.features import FEATURE_NAMES, build_feature_matrix
from fraudlib.rules import evaluate_rules
from fraudlib.schemas import TxnType

PAYSIM_START = datetime(2026, 1, 1, tzinfo=UTC)
CHUNK_ROWS = 500_000


def _hash_ids(col: pd.Series) -> np.ndarray:
    return pd.util.hash_array(col.to_numpy(dtype=object), categorize=False)


def load_columns(path: Path, paysim: bool, limit: int | None) -> tuple[dict, np.ndarray, np.ndarray]:
    """Read the CSV in chunks into one numeric array per column.

    Account, counterparty and device ids are kept as 64-bit hashes of the
    strings, which is all the features need, so 6.3 million PaySim rows fit in
    a few hundred MB instead of several GB of Python objects.
    Returns (columns for build_feature_matrix, event time in ms, labels).
    """
    types = list(TxnType)
    type_code = {t.value: i for i, t in enumerate(types)}
    parts: dict[str, list[np.ndarray]] = {k: [] for k in (
        "t", "account", "counterparty", "device", "has_device", "txn_type", "amount",
        "old_balance_orig", "old_balance_dest", "y")}
    for df in pd.read_csv(path, nrows=limit, chunksize=CHUNK_ROWS):
        if paysim:
            # PaySim: step = hour index. newbalance* columns are post-transaction
            # values (not known at scoring time) and are deliberately not read.
            df = df.rename(columns={"nameOrig": "account_id", "nameDest": "counterparty_id", "type": "txn_type",
                                    "oldbalanceOrg": "old_balance_orig", "oldbalanceDest": "old_balance_dest",
                                    "isFraud": "is_fraud"})
            df["amount"] = df["amount"].clip(lower=0.01)
            parts["t"].append(df["step"].to_numpy(dtype=np.int64))
        else:
            # Same arithmetic as TransactionEvent.event_ms, so both paths agree to the millisecond.
            us = (pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
                  - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(microseconds=1)
            parts["t"].append(np.floor(us.to_numpy(dtype=np.float64) / 1e6 * 1000).astype(np.int64))
        unknown = set(df["txn_type"].unique()) - set(type_code)
        if unknown:
            raise ValueError(f"unknown txn_type values: {sorted(unknown)}")
        for name in ("amount", "old_balance_orig", "old_balance_dest"):
            col = df[name].to_numpy(dtype=np.float64)
            bad = ~np.isfinite(col) | (col < 0)
            if name == "amount":
                bad |= (col <= 0) | (col >= 1e9)
            if bad.any():
                raise ValueError(f"{name} has values outside what TransactionEvent accepts")
            parts[name].append(col)
        has_device = df["device_id"].notna().to_numpy() if "device_id" in df else np.zeros(len(df), dtype=bool)
        parts["has_device"].append(has_device)
        parts["device"].append(_hash_ids(df["device_id"].fillna("")) if "device_id" in df
                               else np.zeros(len(df), dtype=np.uint64))
        parts["account"].append(_hash_ids(df["account_id"]))
        parts["counterparty"].append(_hash_ids(df["counterparty_id"]))
        parts["txn_type"].append(df["txn_type"].map(type_code).to_numpy(dtype=np.int8))
        parts["y"].append(df["is_fraud"].to_numpy(dtype=np.int64))
    cols = {k: np.concatenate(v) for k, v in parts.items()}
    t = cols.pop("t")
    if paysim:
        # One draw for the whole file, as before, so a given --limit always gets the same times.
        secs = np.random.default_rng(0).integers(0, 3600, len(t))
        t = int(PAYSIM_START.timestamp() * 1000) + t * 3_600_000 + secs * 1000
    y = cols.pop("y")
    return cols, t, y


def rules_scores(X: np.ndarray, block: int = 100_000) -> np.ndarray:
    """Rule score per row, built a block at a time so the row dicts never all exist at once."""
    out = np.empty(len(X))
    for start in range(0, len(X), block):
        rows = X[start:start + block].tolist()
        out[start:start + block] = [evaluate_rules(dict(zip(FEATURE_NAMES, r, strict=True)))[0] for r in rows]
    return out


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
    ap.add_argument("--device", choices=["cpu", "cuda"], default=os.environ.get("TRAIN_DEVICE", "cpu"),
                    help="where XGBoost trains (cuda = NVIDIA GPU; XGBoost's pip package includes CUDA). "
                         "Features, rules, logistic regression and random forest always use the CPU.")
    ap.add_argument("--lightgbm-device", choices=["cpu", "gpu", "cuda"], default="cpu",
                    help="gpu/cuda need a LightGBM built from source with GPU support; the pip build is CPU only")
    a = ap.parse_args()

    cols, times, y = load_columns(a.data, a.paysim, a.limit)
    X = pd.DataFrame(build_feature_matrix(event_ms=times, **cols), columns=FEATURE_NAMES)
    del cols
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
                                       # Caps each leaf's output. Without it, a class weight in the
                                       # thousands (PaySim: ~1,800) lets leaf values run away and the
                                       # scores saturate at 1.0.
                                       max_delta_step=1.0,
                                       device_type=a.lightgbm_device,
                                       random_state=0, verbose=-1),
    }
    try:
        import xgboost as xgb
    except ImportError:
        xgb = None
        print("xgboost not installed: skipping it (pip install xgboost)")
    if xgb is not None:
        # Same settings as LightGBM where they mean the same thing, so the two compare fairly.
        models["xgboost"] = xgb.XGBClassifier(
            n_estimators=400, learning_rate=0.05, max_depth=8, min_child_weight=5, subsample=0.8,
            colsample_bytree=0.8, scale_pos_weight=pos_weight, max_delta_step=1.0,
            tree_method="hist", device=a.device, eval_metric="aucpr", random_state=0)
    elif a.device == "cuda":
        sys.exit("--device cuda trains XGBoost on the GPU, but xgboost is not installed")
    results, fitted, fit_seconds = {}, {}, {}
    for name, model in models.items():
        if model is None:
            score_fn = lambda df: rules_scores(df.to_numpy())
        else:
            t_fit = time.perf_counter()
            model.fit(X[tr], y[tr])
            fit_seconds[name] = round(time.perf_counter() - t_fit, 1)
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
        "devices": {"xgboost": a.device if "xgboost" in fitted else None, "lightgbm": a.lightgbm_device},
        "fit_seconds": fit_seconds,
    }
    (out_dir / "model_meta.json").write_text(json.dumps(meta, indent=2))
    # What the model's inputs looked like in training, for drift monitoring at run time.
    reference = build_reference(X[tr].to_numpy(), FEATURE_NAMES)
    reference.update({"model_version": a.version, "data": str(a.data), "split": "train (first 70% by time)",
                      "built_at": meta["trained_at"]})
    (out_dir / "reference.json").write_text(json.dumps(reference, indent=2))
    Path("reports").mkdir(exist_ok=True)
    report = {"meta": meta, "results": results, "lightgbm_gain_importance": importance,
              "caveat": ("PaySim: a public simulator of mobile-money transfers, not real UPI or bank data; "
                         "not real-world performance." if a.paysim else
                         "Synthetic data from this project's own simulator; not real-world performance.")}
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
