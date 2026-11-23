"""Train chronological, per-machine next-vibration predictors.

Run from the repository root:
    python -m modeling.train_model

The model predicts the next vibration reading from the preceding six readings
of the same machine. Historical sessions are separated by a 15-second gap.
This is a demo on synthetic sensor data, not a safety-certified fault detector.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

LAGS = 6
MAX_GAP_SECONDS = 15
MIN_TRAIN_SAMPLES = 12
NORMAL_VIBRATION_MIN = 0.5
NORMAL_VIBRATION_MAX = 3.0  # heuristic training filter, not a ground-truth label
MIN_ERROR_THRESHOLD = 0.35

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA = ROOT / "data" / "machine_events.csv"
DEFAULT_MODEL = ROOT / "modeling" / "machine_vibration_model.joblib"
DEFAULT_REPORT = ROOT / "modeling" / "model_report.json"


def build_examples(events: pd.DataFrame) -> pd.DataFrame:
    """Build independent per-device examples without crossing collection gaps."""
    required = {
        "event_id", "event_time", "machine_id", "vibration_mm_s",
        "operating_state",
    }
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

    events = events.copy()
    events["event_time"] = pd.to_datetime(events["event_time"], utc=True, errors="raise")
    events["vibration_mm_s"] = pd.to_numeric(events["vibration_mm_s"], errors="raise")
    events = events.sort_values(["machine_id", "event_time", "event_id"])

    records = []
    for machine_id, machine in events.groupby("machine_id", sort=True):
        machine = machine.reset_index(drop=True)
        gap = machine["event_time"].diff().dt.total_seconds().gt(MAX_GAP_SECONDS)
        session_ids = gap.cumsum()
        for _, session in machine.groupby(session_ids, sort=False):
            readings = session["vibration_mm_s"].to_numpy(dtype=float)
            states = session["operating_state"].astype(str).str.lower().to_numpy()
            for i in range(LAGS, len(session)):
                if not np.isfinite(readings[i - LAGS:i + 1]).all():
                    continue
                records.append({
                    "event_id": str(session.iloc[i]["event_id"]),
                    "event_time": session.iloc[i]["event_time"],
                    "machine_id": str(machine_id),
                    "target": float(readings[i]),
                    "all_running": bool(np.all(states[i - LAGS:i + 1] == "running")),
                    **{f"lag_{j}": float(readings[i - j]) for j in range(1, LAGS + 1)},
                })

    if not records:
        raise ValueError("No sequence examples: need 7 consecutive readings per machine/session.")
    return pd.DataFrame(records).sort_values(["machine_id", "event_time", "event_id"])


def train(data_path: Path, model_path: Path, report_path: Path) -> None:
    data = pd.read_csv(data_path, encoding="utf-8-sig")
    examples = build_examples(data)
    feature_names = [f"lag_{i}" for i in range(1, LAGS + 1)]

    model_by_machine = {}
    machine_report = {}
    total_holdout = 0
    total_flags = 0

    for machine_id, group in examples.groupby("machine_id", sort=True):
        group = group.sort_values(["event_time", "event_id"]).reset_index(drop=True)
        # Use earlier observations for training, later observations for evaluation.
        split = int(len(group) * 0.70)
        train_pool = group.iloc[:split].copy()
        holdout = group.iloc[split:].copy()

        # Exclude simulated fault/idle readings from normal-model training.
        normal_train = train_pool[
            train_pool["all_running"]
            & train_pool["target"].between(NORMAL_VIBRATION_MIN, NORMAL_VIBRATION_MAX)
            & train_pool[feature_names].ge(NORMAL_VIBRATION_MIN).all(axis=1)
            & train_pool[feature_names].le(NORMAL_VIBRATION_MAX).all(axis=1)
        ].copy()
        if len(normal_train) < MIN_TRAIN_SAMPLES or len(holdout) == 0:
            raise ValueError(
                f"{machine_id}: insufficient data ({len(normal_train)} normal training, "
                f"{len(holdout)} holdout examples). Generate more sequential data."
            )

        model = make_pipeline(StandardScaler(), Ridge(alpha=5.0))
        model.fit(normal_train[feature_names], normal_train["target"])

        train_predictions = model.predict(normal_train[feature_names])
        train_errors = np.abs(normal_train["target"].to_numpy() - train_predictions)
        # Calibrated on training errors only (never on the holdout set).
        error_threshold = float(max(MIN_ERROR_THRESHOLD, np.quantile(train_errors, 0.99)))

        predictions = model.predict(holdout[feature_names])
        actual = holdout["target"].to_numpy(dtype=float)
        abs_errors = np.abs(actual - predictions)
        anomaly_flags = abs_errors > error_threshold
        mae = float(mean_absolute_error(actual, predictions))
        rmse = float(np.sqrt(mean_squared_error(actual, predictions)))

        model_by_machine[machine_id] = {
            "model": model,
            "error_threshold": error_threshold,
        }
        machine_report[machine_id] = {
            "total_sequence_examples": int(len(group)),
            "train_pool_size": int(len(train_pool)),
            "normal_training_examples": int(len(normal_train)),
            "holdout_examples": int(len(holdout)),
            "holdout_mae_mm_s": round(mae, 4),
            "holdout_rmse_mm_s": round(rmse, 4),
            "error_threshold_mm_s": round(error_threshold, 4),
            "holdout_anomaly_flags": int(anomaly_flags.sum()),
        }
        total_holdout += len(holdout)
        total_flags += int(anomaly_flags.sum())
        print(
            f"{machine_id}: train_normal={len(normal_train)}, "
            f"test={len(holdout)}, MAE={mae:.3f}, RMSE={rmse:.3f}, "
            f"threshold={error_threshold:.3f}, flagged={int(anomaly_flags.sum())}"
        )

    bundle = {
        "version": 1,
        "lag_count": LAGS,
        "max_gap_seconds": MAX_GAP_SECONDS,
        "feature_names": feature_names,
        "models": model_by_machine,
        "method": "per-machine StandardScaler + Ridge one-step-ahead vibration regression",
    }
    report = {
        "method": bundle["method"],
        "data_source": str(data_path.name),
        "evaluation": "chronological 70/30 holdout per machine; normal training subset only",
        "warning": (
            "Synthetic demonstration. Flags are prediction-error outliers, not "
            "verified physical machine failures; holdout includes synthetic fault periods."
        ),
        "total_holdout_examples": total_holdout,
        "total_holdout_anomaly_flags": total_flags,
        "machines": machine_report,
    }
    model_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, model_path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nSaved model: {model_path}")
    print(f"Saved report: {report_path}")
    print("NOTE: Evaluate MAE/RMSE and flags before describing model quality or deploying.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    train(args.data, args.model, args.report)


if __name__ == "__main__":
    main()
