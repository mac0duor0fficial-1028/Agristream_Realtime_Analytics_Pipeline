import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "agri.sensor.readings")
RATE = float(os.getenv("EVENTS_PER_SECOND", "10"))

FIELDS = {
    "field-01": "maize",
    "field-02": "wheat",
    "field-03": "coffee",
    "field-04": "tea",
    "field-05": "beans",
}

state = {
    fid: {"soil": 32.0, "temp": 24.0, "hum": 60.0, "ph": 6.5}
    for fid in FIELDS
}


def walk(value, step, low, high):
    return round(min(high, max(low, value + random.uniform(-step, step))), 2)


def make_reading():
    fid = random.choice(list(FIELDS))
    s = state[fid]
    s["soil"] = walk(s["soil"], 1.5, 12, 50)
    s["temp"] = walk(s["temp"], 0.8, 10, 42)
    s["hum"] = walk(s["hum"], 2.0, 25, 95)
    s["ph"] = walk(s["ph"], 0.05, 5.0, 8.0)

    ts = datetime.now(timezone.utc)
    if random.random() < 0.05:  # simulate a late-arriving packet
        ts -= timedelta(seconds=random.uniform(0.5, 3))

    return fid, {
        "sensor_id": f"{fid}-s{random.randint(1, 3)}",
        "field_id": fid,
        "crop": FIELDS[fid],
        "soil_moisture": s["soil"],
        "temperature_c": s["temp"],
        "humidity": s["hum"],
        "ph": s["ph"],
        "event_time": ts.replace(tzinfo=None).isoformat(timespec="milliseconds"),
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 50,
    "compression.type": "lz4",
})

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

print(f"Producing to {TOPIC} at ~{RATE}/s via {BOOTSTRAP}", flush=True)
sent = 0
while running:
    key, payload = make_reading()
    producer.produce(TOPIC, key=key, value=json.dumps(payload), on_delivery=on_delivery)
    producer.poll(0)
    sent += 1
    if sent % 100 == 0:
        print(f"sent {sent} events", flush=True)
    time.sleep(1.0 / RATE)

producer.flush(10)
print("Producer stopped cleanly.", flush=True)