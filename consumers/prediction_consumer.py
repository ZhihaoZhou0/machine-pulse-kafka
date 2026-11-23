"""Ridge-AR6 prediction consumer with durable per-machine sequence state.

Run after applying sql/predictions.sql:
    python -m consumers.prediction_consumer

The prediction row and updated six-reading state are committed in ONE
PostgreSQL transaction. Only afterward is the Kafka offset committed.
If a Kafka message is replayed after a crash, its event timestamp is
no newer than the saved state, so it cannot advance the sequence twice.

On the first run of this version, devices warm up from a clean six-reading
history. This is intentional: raw-storage may be ahead of this consumer,
so borrowing its newest rows can introduce future observations.
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

INSERT_PREDICTION_SQL = """
INSERT INTO machine_predictions (
    event_id, machine_id, event_time, actual_vibration,
    predicted_vibration, absolute_error, anomaly_threshold,
    is_anomaly, model_name
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

UPSERT_STATE_SQL = """
INSERT INTO machine_prediction_state (
    machine_id, last_event_id, last_event_time, recent_readings
) VALUES (%s, %s, %s, %s::jsonb)
ON CONFLICT (machine_id) DO UPDATE SET
    last_event_id = EXCLUDED.last_event_id,
    last_event_time = EXCLUDED.last_event_time,
    recent_readings = EXCLUDED.recent_readings,
    updated_at = NOW()
"""


def timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Event timestamp must include a timezone")
    return dt


def advance_sequence(history, event_time, actual, lag_count, max_gap):
    """Pure sequential step; target is appended only AFTER inference inputs."""
    history = deque(history, maxlen=lag_count)
    if history:
        delta = (event_time - history[-1][0]).total_seconds()
        if delta <= 0:
            return "skipped", history, None
        if delta > max_gap:
            history.clear()
    features = None
    if len(history) == lag_count:
        features = [float(history[-j][1]) for j in range(1, lag_count + 1)]
    history.append((event_time, float(actual)))
    return ("predicted" if features is not None else "warmup"), history, features


def process_event(conn, event, model_bundle):
    """Atomically persist model inference and the state used for recovery."""
    machine_id = str(event["machine_id"])
    if machine_id not in model_bundle["models"]:
        raise ValueError(f"No trained model for machine {machine_id!r}")

    event_id = str(event["event_id"])
    event_time = timestamp(str(event["event_time"]))
    actual = float(event["vibration_mm_s"])
    lag_count = int(model_bundle["lag_count"])
    max_gap = float(model_bundle["max_gap_seconds"])
    predicted_result = None

    with conn.transaction():
        with conn.cursor() as cur:
            # The row lock serializes state changes if accidental parallel
            # workers process the SAME machine; this demo uses one consumer.
            cur.execute(
                """SELECT last_event_time, recent_readings
                   FROM machine_prediction_state
                   WHERE machine_id = %s FOR UPDATE""",
                (machine_id,),
            )
            row = cur.fetchone()
            if row:
                last_time, raw_readings = row
                # psycopg 3 normally decodes JSONB into Python objects.
                if isinstance(raw_readings, str):
                    raw_readings = json.loads(raw_readings)
                history = [
                    (timestamp(item["event_time"]), float(item["vibration_mm_s"]))
                    for item in raw_readings
                ]
                if event_time <= last_time:
                    # Already applied or out of order: skip without changing
                    # durable state. Safe to commit Kafka offset afterward.
                    return "skipped", None
            else:
                history = []

            status, updated_history, features = advance_sequence(
                history, event_time, actual, lag_count, max_gap
            )
            if status == "skipped":
                return "skipped", None
            if features is not None:
                fitted = model_bundle["models"][machine_id]
                feature_frame = pd.DataFrame(
                    [features], columns=model_bundle["feature_names"]
                )
                expected = float(fitted["model"].predict(feature_frame)[0])
                threshold = float(fitted["error_threshold"])
                error = abs(actual - expected)
                anomaly = error > threshold
                cur.execute(
                    INSERT_PREDICTION_SQL,
                    (event_id, machine_id, event_time, actual, expected,
                     error, threshold, anomaly, "Ridge-AR6"),
                )
                predicted_result = (expected, error, anomaly)

            serialized_history = [
                {"event_time": dt.isoformat(), "vibration_mm_s": v}
                for dt, v in updated_history
            ]
            cur.execute(
                UPSERT_STATE_SQL,
                (machine_id, event_id, event_time,
                 json.dumps(serialized_history)),
            )
    return ("predicted" if predicted_result is not None else "warmup"), predicted_result


def main() -> None:
    load_dotenv()
    if not MODEL_FILE.is_file():
        raise FileNotFoundError(
            f"Missing model {MODEL_FILE}; run: python -m modeling.train_model"
        )
    bundle = joblib.load(MODEL_FILE)  # Trusted repository-generated model only.
    if int(bundle.get("lag_count", 0)) != 6:
        raise ValueError("Expected a trained AR(6) model bundle")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in your local .env")

    kafka = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    count = warmups = anomalies = skipped = 0
    try:
        with psycopg.connect(
            host="127.0.0.1", port=5433, dbname="machinepulse",
            user="machinepulse", password=password,
            connect_timeout=5, autocommit=True,
        ) as conn:
            # Check prerequisites before subscribing to Kafka.
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM machine_prediction_state LIMIT 1")
            kafka.subscribe([TOPIC])
            print(f"Listening to {TOPIC} | group={GROUP_ID} | model=Ridge-AR6", flush=True)
            print("Prediction history restored from its own PostgreSQL state. "
                  "New machines need six readings to warm up.", flush=True)
            while True:
                msg = kafka.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                event = json.loads(msg.value().decode("utf-8"))
                status, prediction = process_event(conn, event, bundle)
                # The database transaction already committed successfully.
                # If this offset commit fails, replay is handled by saved state.
                kafka.commit(message=msg, asynchronous=False)
                if status == "warmup":
                    warmups += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    count += 1
                    expected, error, flagged = prediction
                    anomalies += int(flagged)
                    if count <= 5 or count % 10 == 0 or flagged:
                        print(
                            f"[{count}] {event['machine_id']} | "
                            f"actual={float(event['vibration_mm_s']):.3f} | "
                            f"predicted={expected:.3f} | "
                            f"abs_error={error:.3f} | model_anomaly={flagged}",
                            flush=True,
                        )
    except KeyboardInterrupt:
        print("\nPrediction consumer stopped by user.", flush=True)
    finally:
        kafka.close()
        print(f"Predictions={count}, warmups={warmups}, "
              f"skipped={skipped}, model_anomalies={anomalies}", flush=True)


if __name__ == "__main__":
    main()
