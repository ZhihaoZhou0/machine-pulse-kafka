
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


MACHINES = [
    {"id": "CNC-01", "type": "CNC", "base_temp": 52, "base_vibration": 1.8, "base_rpm": 2400},
    {"id": "CNC-02", "type": "CNC", "base_temp": 55, "base_vibration": 2.0, "base_rpm": 2600},
    {"id": "PRESS-01", "type": "Press", "base_temp": 43, "base_vibration": 2.6, "base_rpm": 900},
    {"id": "PRESS-02", "type": "Press", "base_temp": 46, "base_vibration": 2.4, "base_rpm": 950},
    {"id": "LATHE-01", "type": "Lathe", "base_temp": 49, "base_vibration": 1.7, "base_rpm": 1800},
]

rng = random.Random()
machine_offsets = {m["id"]: {"temp": 0.0, "vibration": 0.0} for m in MACHINES}


def make_event(machine):
    machine_id = machine["id"]
    offsets = machine_offsets[machine_id]

    # Smooth variation across successive readings rather than independent noise.
    offsets["temp"] = max(-4, min(4, offsets["temp"] + rng.gauss(0, 0.35)))
    offsets["vibration"] = max(-0.5, min(0.5, offsets["vibration"] + rng.gauss(0, 0.07)))

    operating_state = rng.choices(
        ["running", "idle"], weights=[0.92, 0.08], k=1
    )[0]

    if operating_state == "running":
        temperature = machine["base_temp"] + offsets["temp"] + rng.gauss(0, 0.3)
        vibration = machine["base_vibration"] + offsets["vibration"] + rng.gauss(0, 0.05)
        rpm = machine["base_rpm"] + rng.randint(-60, 60)
    else:
        temperature = 28 + rng.uniform(-2, 2)
        vibration = 0.2 + rng.uniform(0, 0.15)
        rpm = 0

    # A repeatable, slowly increasing vibration problem on one machine.
    if machine_id == "CNC-02" and operating_state == "running":
        elapsed = time.monotonic() - START_TIME
        vibration += min(4.0, max(0, elapsed - 60) / 50)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "machine_id": machine_id,
        "machine_type": machine["type"],
        "operating_state": operating_state,
        "temperature_c": round(temperature, 2),
        "vibration_mm_s": round(vibration, 3),
        "rpm": max(0, int(rpm)),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Machine Pulse Kafka Producer")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=int, default=0, help="Seconds; 0 means continuous")
    args = parser.parse_args()

    if args.interval <= 0 or args.duration < 0:
        parser.error("--interval must be positive and --duration cannot be negative")

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "machine-pulse-producer",
    })

    global START_TIME
    START_TIME = time.monotonic()
    count = 0

    print("Streaming events to machine-telemetry. Press Ctrl+C to stop.")

    try:
        while args.duration == 0 or time.monotonic() - START_TIME < args.duration:
            machine = MACHINES[count % len(MACHINES)]
            event = make_event(machine)

            producer.produce(
                "machine-telemetry",
                key=event["machine_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            count += 1
            if count <= 5 or count % 10 == 0:
                print(
                    f"[{count}] {event['machine_id']} | "
                    f"temp={event['temperature_c']}°C | "
                    f"vibration={event['vibration_mm_s']} mm/s",
                    flush=True,
                )

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        remaining = producer.flush(15)
        if remaining:
            print(f"Warning: {remaining} messages were not confirmed delivered.")
        print(f"Generated {count} events.")


if __name__ == "__main__":
    START_TIME = time.monotonic()
    main()
