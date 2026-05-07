from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np


def _row_from_event(d: dict[str, Any]) -> np.ndarray:
    wear_avg = (
        float(d["tire_wear_fl"])
        + float(d["tire_wear_fr"])
        + float(d["tire_wear_rl"])
        + float(d["tire_wear_rr"])
    ) / (4.0 * 100.0)
    temp_avg = (
        float(d["tire_temp_fl"])
        + float(d["tire_temp_fr"])
        + float(d["tire_temp_rl"])
        + float(d["tire_temp_rr"])
    ) / (4.0 * 150.0)
    fuel = float(d["fuel_load_kg"]) / 120.0
    speed = float(d["speed_kph"]) / 380.0
    ers = float(d["ers_deploy_pct"]) / 100.0
    lap = min(int(d["lap"]), 100) / 100.0
    row = np.array(
        [[wear_avg, temp_avg, fuel, speed, ers, lap]], dtype=np.float64
    )
    return np.clip(row, 0.0, 1.0)


class Predictor:
    def __init__(self, model_path: str) -> None:
        path = Path(model_path)
        if not path.is_file():
            raise FileNotFoundError(f"Model not found: {path}")
        self._clf = joblib.load(path)

    def pit_probability(self, event: dict[str, Any]) -> float:
        x = _row_from_event(event)
        prob = self._clf.predict_proba(x)[0, 1]
        return float(prob)
