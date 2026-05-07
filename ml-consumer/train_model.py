#!/usr/bin/env python3
"""Train a small RandomForest on synthetic features matching ml-consumer inference columns."""
from __future__ import annotations

import os

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier

FEATURE_DIM = 6
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "pit_rf.pkl")


def main() -> None:
    rng = np.random.default_rng(42)
    n = 10_000
    x = rng.random((n, FEATURE_DIM), dtype=np.float64)
    # Pit likely when high wear (col0) or combined wear + low fuel (col2)
    wear = x[:, 0]
    fuel = x[:, 2]
    y = ((wear > 0.72) | ((wear > 0.52) & (fuel < 0.28))).astype(np.int32)
    clf = RandomForestClassifier(
        n_estimators=64,
        max_depth=12,
        min_samples_leaf=4,
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(x, y)
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(clf, MODEL_PATH)
    print(f"Wrote {MODEL_PATH}")


if __name__ == "__main__":
    main()
