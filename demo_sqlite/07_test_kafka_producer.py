#!/usr/bin/env python3
# test_kafka_producer.py

"""
Kafka Test Producer for Debezium-style CDC Events
Sends sample insert/update/delete events to the 'sqlite.cdc.items' topic
so that your postgres_cdc_sink.py can consume and apply them.
"""

import os
import json
import time
from kafka import KafkaProducer

# Configuration: adjust if needed
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sqlite.cdc.items")

def make_debezium_record(op, before, after):
    """Wrap operation in Debezium envelope format."""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "before": before,
        "after": after,
        "op": op,
        "ts_ms": int(time.time() * 1000),
        "source": {
            "version": "1.9.0.Final",
            "connector": "sqlite",
            "name": "sqlite",
            "db": "test_cdc",
            "table": "items",
            "ts_ms": int(time.time() * 1000)
        }
    }
    return {"payload": payload}

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # 1) Simulate INSERT
    rec1 = make_debezium_record(
        op="c",
        before=None,
        after={"id": 1, "name": "Alpha"}
    )
    producer.send(TOPIC, rec1)
    print("[Producer] Sent INSERT record for id=1")
    time.sleep(1)

    # 2) Simulate UPDATE
    rec2 = make_debezium_record(
        op="u",
        before={"id": 1, "name": "Alpha"},
        after={"id": 1, "name": "AlphaUpdated"}
    )
    producer.send(TOPIC, rec2)
    print("[Producer] Sent UPDATE record for id=1")
    time.sleep(1)

    # 3) Simulate DELETE
    rec3 = make_debezium_record(
        op="d",
        before={"id": 1, "name": "AlphaUpdated"},
        after=None
    )
    producer.send(TOPIC, rec3)
    print("[Producer] Sent DELETE record for id=1")

    # Ensure all messages are sent
    producer.flush()
    print("All test events sent to topic:", TOPIC)

if __name__ == "__main__":
    print("Starting Kafka test producer...")
    main()
    print("Producer finished.")

