import os
import json
import sys
import datetime
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException
from shared.config import load_dotenv
from shared.db import build_dsn_from_env

load_dotenv()
DSN = build_dsn_from_env()
KAFKA_TOPIC_SILVER_SITES = os.getenv("KAFKA_TOPIC_SILVER_SITES", "kt.silver.sites")

def get_kafka_consumer():
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": broker,
        "group.id": "silver_sites_loader_group_v3",  
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)

def save_sites(cur, records):
    nct_ids = list(set(r["nct_id"] for r in records if r.get("nct_id")))
    if nct_ids:
        cur.execute("DELETE FROM silver.trial_sites WHERE nct_id = ANY(%s)", (nct_ids,))
    
    now_ts = datetime.datetime.now()
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO silver.trial_sites (
            nct_id, facility_name, city, state, zip, country,
            latitude, longitude, mesh_conditions_ids, transformed_at
        ) VALUES %s
        """,
        [
            (
                r["nct_id"], r["facility_name"], r["city"], r["state"], r["zip"], r["country"],
                r["latitude"], r["longitude"], r.get("mesh_conditions_ids", []), now_ts
            )
            for r in records
        ],
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    )

def flush_buffer(records):
    if not records: return
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                # Forza Postgres ad attendere la fine della transazione prima di verificare la FK
                cur.execute("SET CONSTRAINTS ALL DEFERRED;")
                save_sites(cur, records)
        print(f"[INFO DB - SITES]: Sincronizzato bulk di {len(records)} siti clinici.")
    except Exception as e:
        print(f"[ERR DB - SITES]: Errore durante il flush: {e}")
        raise e
    finally:
        conn.close()

def main():
    consumer = get_kafka_consumer()
    consumer.subscribe([KAFKA_TOPIC_SILVER_SITES])
    print(f"[START]: Consumer SITES active on {KAFKA_TOPIC_SILVER_SITES}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = []

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            if not messages:
                if buffer:
                    print(f"[TIMEOUT]: Flusho residuo di {len(buffer)} siti...")
                    flush_buffer(buffer)
                    consumer.commit(asynchronous=False)
                    buffer.clear()
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF: continue
                    else: raise KafkaException(msg.error())
                
                try:
                    kafka_envelope = json.loads(msg.value().decode("utf-8"))
                    if isinstance(kafka_envelope, dict) and "value" in kafka_envelope:
                        actual_record = json.loads(kafka_envelope["value"]) if isinstance(kafka_envelope["value"], str) else kafka_envelope["value"]
                    else:
                        actual_record = kafka_envelope
                    
                    if "nct_id" in actual_record:
                        buffer.append(actual_record)
                except Exception as parse_err:
                    print(f"[ERR PARSING SITES]: {parse_err}")

            if buffer:
                print(f"[BATCH]: Invio {len(buffer)} siti al DB...")
                flush_buffer(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Consumer arrestato.")
    except Exception as e:
        print(f"[CRITICAL ERR SITES]: Crash: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()