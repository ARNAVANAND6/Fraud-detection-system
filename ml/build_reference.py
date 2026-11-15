"""Write the drift reference (models/<version>/reference.json) for an existing model.

ml/train.py writes this file for every new model. Use this script for a model
trained before drift monitoring existed: it rebuilds the features from the
training data and stores their distribution, without retraining.

    python ml/build_reference.py --data data/synthetic/transactions.csv --model-dir models/current
    python ml/build_reference.py --paysim --data data/paysim/<file>.csv --model-dir models/<version>

The reference is only meaningful if --data is the data the model was trained on.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "libs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fraudlib.drift import build_reference
from fraudlib.features import FEATURE_NAMES, build_feature_matrix
from train import load_columns, time_split


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--paysim", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--model-dir", type=Path, required=True)
    a = ap.parse_args()
    meta = json.loads((a.model_dir / "model_meta.json").read_text())
    if meta["feature_names"] != FEATURE_NAMES:
        sys.exit("this model was trained on different features")
    cols, times, y = load_columns(a.data, a.paysim, a.limit)
    if len(y) != meta["rows"]:
        sys.exit(f"{a.data} has {len(y)} rows but the model was trained on {meta['rows']}: not the same data")
    X = build_feature_matrix(event_ms=times, **cols)
    tr, _, _ = time_split(times)
    ref = build_reference(X[tr], FEATURE_NAMES)
    ref.update({"model_version": meta["model_version"], "data": str(a.data), "split": "train (first 70% by time)",
                "built_at": datetime.now(UTC).isoformat()})
    (a.model_dir / "reference.json").write_text(json.dumps(ref, indent=2))
    print(f"wrote {a.model_dir / 'reference.json'} from {ref['rows']} training rows")


if __name__ == "__main__":
    main()
