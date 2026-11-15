"""Feature drift: has live traffic moved away from the data the model learned from?

At training time `build_reference` stores, for every model feature, a set of
bin edges and the share of training rows in each bin. At run time `compare`
bins recent live rows the same way and reports the Population Stability Index
(PSI) per feature:

    PSI = sum over bins of (live - train) * ln(live / train)

The usual reading, used here: below 0.10 stable, 0.10 to 0.25 worth watching,
above 0.25 drifted. PSI says the inputs changed. It does not say the model is
now wrong; only fresh labels can say that. It is a prompt to check and, if
needed, retrain.
"""
from __future__ import annotations

import numpy as np

MAX_BINS = 10
WATCH, DRIFTED = 0.10, 0.25
# A bin that is empty on one side would make PSI infinite; this floor is the usual fix.
FLOOR = 1e-4
# Below this many live rows the estimate is too noisy to act on.
MIN_ROWS = 500
# Features that follow the clock; they need a full day of traffic to be comparable.
DAILY_CYCLE = {"hour_of_day"}


def _edges(values: np.ndarray) -> list[float]:
    """Interior bin edges at training quantiles; duplicates collapse (few distinct values)."""
    qs = np.quantile(values, np.linspace(0, 1, MAX_BINS + 1)[1:-1])
    return sorted({float(q) for q in qs})


def _shares(values: np.ndarray, edges: list[float]) -> np.ndarray:
    # A value equal to an edge falls in the lower bin, for training and live rows alike.
    idx = np.searchsorted(np.asarray(edges), values, side="left")
    return np.bincount(idx, minlength=len(edges) + 1) / max(len(values), 1)


def build_reference(X: np.ndarray, names: list[str]) -> dict:
    X = np.asarray(X)
    features = {}
    for j, name in enumerate(names):
        col = X[:, j].astype(np.float64)
        edges = _edges(col)
        features[name] = {"edges": edges, "shares": [round(float(s), 6) for s in _shares(col, edges)]}
    return {"rows": int(X.shape[0]), "features": features}


def psi(expected: np.ndarray, actual: np.ndarray) -> float:
    e = np.clip(np.asarray(expected, dtype=np.float64), FLOOR, None)
    a = np.clip(np.asarray(actual, dtype=np.float64), FLOOR, None)
    return float(np.sum((a - e) * np.log(a / e)))


def _status(value: float) -> str:
    return "DRIFTED" if value > DRIFTED else "WATCH" if value > WATCH else "STABLE"


def compare(reference: dict, rows: list[dict], span_hours: float) -> dict:
    """PSI of each feature in `rows` (feature dicts from stored predictions) against the reference."""
    n = len(rows)
    if n < MIN_ROWS:
        return {"status": "NOT_ENOUGH_DATA", "rows": n, "min_rows": MIN_ROWS, "features": []}
    out = []
    for name, ref in reference["features"].items():
        values = np.array([r[name] for r in rows if r.get(name) is not None], dtype=np.float64)
        if len(values) < MIN_ROWS:
            continue
        value = psi(np.array(ref["shares"]), _shares(values, ref["edges"]))
        needs_full_day = name in DAILY_CYCLE and span_hours < 20
        out.append({"feature": name, "psi": round(value, 4),
                    "status": "NEEDS_24H" if needs_full_day else _status(value)})
    out.sort(key=lambda f: -f["psi"])
    judged = [f for f in out if f["status"] != "NEEDS_24H"]
    worst = max((f["psi"] for f in judged), default=0.0)
    return {"status": _status(worst), "rows": n, "span_hours": round(span_hours, 2),
            "reference_rows": reference["rows"], "watch_above": WATCH, "drifted_above": DRIFTED,
            "features": out}
