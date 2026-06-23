import os
import json
from confluent_kafka import Producer

def build_kafka_producer():
    """Create a Kafka producer from environment config, or None if not configured."""
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not broker:
        print("[INFO]: KAFKA_BROKER/KAFKA_BOOTSTRAP_SERVERS not set; skipping Kafka production.")
        return None
    return Producer({"bootstrap.servers": broker})


def delivery_report(err, msg):
    """Callback per verificare la corretta consegna del messaggio."""
    if err is not None:
        print(f"[ERR KAFKA]: Consegna fallita: {err}")


def produce_study_to_kafka(producer, study):
    """Produce un singolo payload JSON sul topic bronze."""
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
    producer.poll(0)


def produce_silver_partition_to_kafka(rows, topic_name=None):
    """Produce i record trasformati sui topic Silver/Gold dai nodi Worker di Spark."""
    rows = list(rows)
    if not rows:
        return

    producer = build_kafka_producer()
    if producer is None:
        return
        
    topic = topic_name or os.getenv("KAFKA_TOPIC_SILVER", "trials.silver")

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
            
            # Esegue il poll ogni 100 messaggi per evitare l'overhead di I/O sulla CPU del worker
            if idx % 100 == 0:
                producer.poll(0)
            
        # Forza l'invio finale e svuota il buffer della partizione corrente prima di chiudere il task Spark
        producer.flush()
    except Exception as e:
        print(f"[ERR SPARK WORKER]: Errore durante la scrittura parallela su Kafka: {e}")
        raise e