import os
import json
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from shared.config import load_dotenv
from shared.db import build_dsn_from_env, insert_bronze_studies_bulk

load_dotenv()
DSN = build_dsn_from_env()
KAFKA_TOPIC_BRONZE_TRIALS = os.getenv("KAFKA_TOPIC_BRONZE_TRIALS", "kt.bronze.trials")

def get_kafka_consumer():
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": broker,
        "group.id": "clinical_trials_bronze_loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False, 
    }
    return Consumer(conf)

def main():
    consumer = get_kafka_consumer()
    consumer.subscribe([KAFKA_TOPIC_BRONZE_TRIALS])
    print(f"[START]: Consumer BRONZE unificato in ascolto su {KAFKA_TOPIC_BRONZE_TRIALS}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = []

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            if not messages:
                if buffer:
                    insert_bronze_studies_bulk(buffer, dsn=DSN)
                    consumer.commit(asynchronous=False)
                    print(f"[TIMEOUT]: Scritto buffer residuo Bronze di {len(buffer)} record.")
                    buffer.clear()
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                payload = json.loads(msg.value().decode("utf-8"))
                buffer.append(payload)

            if buffer:
                insert_bronze_studies_bulk(buffer, dsn=DSN)
                consumer.commit(asynchronous=False)
                print(f"[INFO DB]: Caricato bulk di {len(buffer)} record in bronze.")
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Consumer Bronze arrestato manualmente.")
    except Exception as e:
        print(f"[CRITICAL ERR]: Errore bloccante nel consumer Bronze: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()