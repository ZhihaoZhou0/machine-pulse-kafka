
import json
import os

import psycopg
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv


TOPIC = "machine-telemetry"
GROUP_ID = "machine-pulse-stream-processing"

WINDOW_SECONDS = 30
VIBRATION_THRESHOLD = 3.0


# Read recent readings already handled by this processing consumer.
# This makes the rolling window recoverable after a restart.
WINDOW_SQL = """
SELECT COUNT(*), AVG(vibration_mm_s)
FROM machine_metrics
WHERE machine_id = %s
  AND event_time >= %s::timestamptz - INTERVAL '30 seconds'
  AND event_time <= %s::timestamptz
"""

INSERT_SQL = """
INSERT INTO machine_metrics (
    event_id,
    machine_id,
    event_time,
    vibration_mm_s,
    rolling_avg_vibration,
    sample_count,
    alert_status
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""


def process_event(conn, event):
    event_id = event["event_id"]
    machine_id = event["machine_id"]
    event_time = event["event_time"]
    vibration = float(event["vibration_mm_s"])

    # Both the read and the insert happen in one DB transaction.
    with conn.transaction():
        with conn.cursor() as cur:
            # Avoid counting a replayed event twice.
            cur.execute(
                "SELECT 1 FROM machine_metrics WHERE event_id = %s",
                (event_id,),
            )

            if cur.fetchone():
                return None

            cur.execute(
                WINDOW_SQL,
                (machine_id, event_time, event_time),
            )

            previous_count, previous_average = cur.fetchone()

            # Add the incoming event to the current window.
            previous_total = (
                float(previous_average) * previous_count
                if previous_count
                else 0.0
            )

            sample_count = previous_count + 1
            rolling_average = (
                previous_total + vibration
            ) / sample_count

            alert_status = (
                "WARNING"
                if rolling_average >= VIBRATION_THRESHOLD
                else "NORMAL"
            )

            cur.execute(
                INSERT_SQL,
                (
                    event_id,
                    machine_id,
                    event_time,
                    vibration,
                    rolling_average,
                    sample_count,
                    alert_status,
                ),
            )

    return rolling_average, sample_count, alert_status


def main():
    load_dotenv()

    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD missing from .env")

    print("Starting stream processing consumer...", flush=True)

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    processed = 0

    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=5433,
            dbname="machinepulse",
            user="machinepulse",
            password=password,
            connect_timeout=10,
            autocommit=True,
        ) as conn:

            consumer.subscribe([TOPIC])

            print(
                f"Listening to {TOPIC} | group={GROUP_ID}",
                flush=True,
            )

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode("utf-8"))

                result = process_event(conn, event)

                # Commit Kafka offset only after successful DB processing.
                consumer.commit(message=msg, asynchronous=False)

                if result is None:
                    continue

                processed += 1

                rolling_avg, count, status = result

                if (
                    processed <= 5
                    or processed % 10 == 0
                    or status == "WARNING"
                ):
                    print(
                        f"[{processed}] {event['machine_id']} | "
                        f"rolling_avg={rolling_avg:.3f} mm/s | "
                        f"samples={count} | "
                        f"status={status}",
                        flush=True,
                    )

    except KeyboardInterrupt:
        print("\nProcessing consumer stopped.")

    finally:
        consumer.close()
        print(f"Processed {processed} derived results.")


if __name__ == "__main__":
    main()
