"""Kafka one-step-ahead vibration prediction consumer for Machine Pulse.

Run from the repository root, after applying sql/predictions.sql:
    python -m consumers.prediction_consumer

Start this consumer before starting a NEW producer run. It subscribes to
new messages in its own Kafka consumer group (auto.offset.reset='latest').
The six preceding same-machine readings are seeded from PostgreSQL and then
updated as messages arrive. A gap over 15 seconds resets the sequence.

This is a synthetic-data demonstration, not a certified fault detector.
"""

from __future__ import annotations

import json
import os
from collections import deque
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
MODEL_FILE = ROOT / "modeling" / "machine_vibration_model.joblib"
TOPIC = "machine-telemetry"
GROUP_ID = "machine-pulse-sequential-model"


CREATE_PREDICTION_SQL = """
INSERT INTO machine_predictions (
    event_id, machine_id, event_time,
    actual_vibration, predicted_vibration, absolute_error,
    anomaly_threshold, is_anomaly, model_name
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""


def timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Event timestamp must include a timezone")
    return dt


def bootstrap_history(conn: psycopg.Connection, machine_ids: list[str], lags: int):
    """Load last lags per device from persisted RAW events on startup.

    Only a contiguous tail (<= 15s between readings) is retained. At start,
    the producer should not yet be running to avoid reading future events.
    """
    histories = {machine_id: deque(maxlen=lags) for machine_id in machine_ids}
    with conn.cursor() as cur:
        for machine_id in machine_ids:
            cur.execute(
                """
                SELECT event_time, vibration_mm_s
                FROM machine_events
                WHERE machine_id = %s
                ORDER BY event_time DESC, event_id DESC
                LIMIT %s
                """,
                (machine_id, lags),
            )
            readings = list(reversed(cur.fetchall()))
            for dt, vibration in readings:
                histories[machine_id].append((dt, float(vibration)))
    return histories


def process_event(conn, event, histories, model_bundle):
    machine_id = str(event["machine_id"])
    if machine_id not in model_bundle["models"]:
        raise ValueError(f"No trained model for machine {machine_id!r}")

    event_id = str(event["event_id"])
    event_time = timestamp(str(event["event_time"]))
    actual = float(event["vibration_mm_s"])
    history = histories[machine_id]
    lag_count = int(model_bundle["lag_count"])
    max_gap = float(model_bundle["max_gap_seconds"])

    # Fail fast on non-monotonic readings rather than generate invalid lags.
    if history:
        interval = (event_time - history[-1][0]).total_seconds()
        if interval <= 0:
            # Can occur with a duplicate/replayed event; do not distort history.
            print(f"Skipping duplicate/out-of-order event {event_id} for {machine_id}", flush=True)
            return "skipped", None
        if interval > max_gap:
            history.clear()

    prediction_result = None
    if len(history) == lag_count:
        features = [[float(history[-j][1]) for j in range(1, lag_count + 1)]]
        fitted = model_bundle["models"][machine_id]
        feature_frame = pd.DataFrame(
            features,
            columns=model_bundle["feature_names"]
        )
        expected = float(fitted["model"].predict(feature_frame)[0])
        threshold = float(fitted["error_threshold"])
        error = abs(actual - expected)
        anomaly = error > threshold

        # Database write must succeed before committing the Kafka offset.
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    CREATE_PREDICTION_SQL,
                    (
                        event_id, machine_id, event_time,
                        actual, expected, error,
                        threshold, anomaly, "Ridge-AR6",
                    ),
                )
        prediction_result = (expected, error, anomaly)

    # Append only AFTER prediction, so the target cannot leak into its lags.
    history.append((event_time, actual))
    if prediction_result is None:
        return "warmup", None
    return "predicted", prediction_result


def main():
    load_dotenv()
    if not MODEL_FILE.is_file():
        raise FileNotFoundError(
            f"Missing {MODEL_FILE}. Run: python -m modeling.train_model"
        )
    bundle = joblib.load(MODEL_FILE)  # Only load this trusted, locally trained file.
    if int(bundle.get("lag_count", 0)) != 6:
        raise ValueError("Expected a trained AR(6) bundle")

    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in your local .env")

    kafka = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    count = 0
    warmups = 0
    anomalies = 0
    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=5433,
            dbname="machinepulse",
            user="machinepulse",
            password=password,
            connect_timeout=5,
            autocommit=True,
        ) as conn:
            histories = bootstrap_history(
                conn, list(bundle["models"]), int(bundle["lag_count"])
            )
            kafka.subscribe([TOPIC])
            print(
                f"Listening to {TOPIC} | group={GROUP_ID} | model=Ridge-AR6",
                flush=True,
            )
            print(
                "Start the producer in a separate terminal now. "
                "A gap >15s resets the six-sample history.",
                flush=True,
            )
            while True:
                msg = kafka.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode("utf-8"))
                state, prediction = process_event(conn, event, histories, bundle)
                kafka.commit(message=msg, asynchronous=False)

                if state == "warmup":
                    warmups += 1
                elif state == "predicted":
                    count += 1
                    expected, error, flagged = prediction
                    anomalies += int(flagged)
                    if count <= 5 or count % 10 == 0 or flagged:
                        print(
                            f"[{count}] {event['machine_id']} | "
                            f"actual={float(event['vibration_mm_s']):.3f} | "
                            f"predicted={expected:.3f} | "
                            f"abs_error={error:.3f} | "
                            f"model_anomaly={flagged}",
                            flush=True,
                        )
    except KeyboardInterrupt:
        print("\nPrediction consumer stopped by user.", flush=True)
    finally:
        kafka.close()
        print(
            f"Predictions={count}, warmups={warmups}, model_anomalies={anomalies}",
            flush=True,
        )


if __name__ == "__main__":
    main()
