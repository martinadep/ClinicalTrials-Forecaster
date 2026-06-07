import os
import json
from confluent_kafka import Producer

def build_kafka_producer():
    """Create a Kafka producer from environment config, or None if not configured."""
    broker = os.getenv("KAFKA_BROKER")
    if not broker:
        print("[INFO]: KAFKA_BROKER not set; skipping Kafka production.")
        return None
    return Producer({"bootstrap.servers": broker})


def delivery_report(err, msg):
    """Callback called once for each produced message to confirm delivery."""
    if err is not None:
        print(f"[ERR]: Kafka delivery failed: {err}")
    else:
        print(f"[INFO]: Produced to {msg.topic()} partition {msg.partition()}")


def produce_study_to_kafka(producer, study):
    """Produce a single study payload to the bronze topic."""
    if producer is None:
        return
    topic = os.getenv("KAFKA_TOPIC_BRONZE", "trials.bronze")
    nct_id = (
        study.get("protocolSection", {})
        .get("identificationModule", {})
        .get("nctId")
    )
    payload = json.dumps(study, ensure_ascii=False)
    producer.produce(
        topic,
        key=nct_id.encode("utf-8") if nct_id else None,
        value=payload.encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)  # triggers delivery callbacks