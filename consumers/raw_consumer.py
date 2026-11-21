
import json
import os

import psycopg
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv


TOPIC = "machine-telemetry"
GROUP_ID = "machine-pulse-raw-storage"

INSERT_SQL = """
INSERT INTO machine_events (
    event_id,
    event_time,
    machine_id,
    machine_type,
    operating_state,
    temperature_c,
    vibration_mm_s,
    rpm
)
VALUES (
    %(event_id)s,
    %(event_time)s,
    %(machine_id)s,
    %(machine_type)s,
    %(operating_state)s,
    %(temperature_c)s,
    %(vibration_mm_s)s,
    %(rpm)s
)
ON CONFLICT (event_id) DO NOTHING
"""


def main():
    load_dotenv()

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing from .env")

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
            connect_timeout=5,
        ) as conn:

            consumer.subscribe([TOPIC])

            print(f"Listening to {TOPIC} | group={GROUP_ID}", flush=True)

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode("utf-8"))

                # Database transaction commits before Kafka offset.
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(INSERT_SQL, event)

                consumer.commit(message=msg, asynchronous=False)

                processed += 1

                if processed <= 5 or processed % 10 == 0:
                    print(
                        f"[{processed}] Stored event "
                        f"{event['event_id']} "
                        f"from {event['machine_id']}",
                        flush=True,
                    )

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        print(f"Processed {processed} Kafka messages.")


if __name__ == "__main__":
    main()
