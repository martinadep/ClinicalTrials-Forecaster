import os
import json
from confluent_kafka import Producer

def build_kafka_producer():
    is_docker = os.path.exists('/.dockerenv')
    
    if is_docker:
        broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BROKER")
    else:
        broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS")

    if not broker:
        print("[INFO]: Kafka broker not configured; not sending.")
        return None
        
    print(f"[INFO]: Connected to Kafka through: {broker}")
    return Producer({"bootstrap.servers": broker})

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERR KAFKA]: Failed Delivery: {err}")

def produce_study_to_kafka(producer, study):
    if producer is None:
        return
    topic = os.getenv("KAFKA_TOPIC_BRONZE_TRIALS", "kt.bronze.trials")
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
    producer.poll(0)

def produce_silver_partition_to_kafka(rows, topic_name=None):
    rows = list(rows)
    if not rows:
        return

    producer = build_kafka_producer()
    if producer is None:
        return
        
    topic = topic_name or os.getenv("KAFKA_TOPIC_SILVER_TRIALS", "kt.silver.trials")

    try:
        for idx, row in enumerate(rows):
            trial_dict = row.asDict()
            nct_id = trial_dict.get("nct_id")
            payload = json.dumps(trial_dict, ensure_ascii=False)
            
            producer.produce(
                topic,
                key=nct_id.encode("utf-8") if nct_id else None,
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            
            if idx % 50 == 0:
                producer.poll(0)
            
        producer.flush()
    except Exception as e:
        print(f"[ERR SPARK WORKER]: Errore durante la scrittura parallela su Kafka: {e}")
        raise e