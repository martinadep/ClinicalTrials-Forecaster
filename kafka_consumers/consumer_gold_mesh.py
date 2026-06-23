import os
import json
import sys
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException
from shared.config import load_dotenv
from shared.db import build_dsn_from_env

load_dotenv()
DSN = build_dsn_from_env()
TOPIC = "mesh.gold"

def get_kafka_consumer():
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": broker,
        "group.id": "gold_mesh_loader_group_v2",  # ID Unico
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)

def save_mesh(cur, records):
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO gold.dim_mesh_conditions (mesh_condition_id, mesh_condition_name)
        VALUES %s
        ON CONFLICT (mesh_condition_id) DO NOTHING
        """,
        [
            (r["mesh_condition_id"], r["mesh_condition_name"])
            for r in records if "mesh_condition_id" in r
        ],
        template="(%s, %s)",
    )

def flush_buffer(records):
    if not records: return
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                save_mesh(cur, records)
        print(f"[INFO DB - MESH]: Aggiornate {len(records)} dimensioni MeSH.")
    except Exception as e:
        print(f"[ERR DB - MESH]: Errore durante il flush: {e}")
        raise e
    finally:
        conn.close()

def main():
    consumer = get_kafka_consumer()
    consumer.subscribe([TOPIC])
    print(f"[START]: Consumer MESH attivo su {TOPIC}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = []

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            # Se non ci sono nuovi messaggi, controlla ed esegui il flush del residuo
            if not messages:
                if buffer:
                    print(f"[TIMEOUT]: Flusho residuo di {len(buffer)} record...")
                    flush_buffer(buffer)
                    consumer.commit(asynchronous=False)
                    buffer.clear()
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF: 
                        continue
                    else: 
                        raise KafkaException(msg.error())
                
                try:
                    # 1. Decodifica il payload stringa di Kafka
                    raw_str = msg.value().decode("utf-8")
                    kafka_envelope = json.loads(raw_str)
                    
                    # 2. Estrazione basata sull'involucro reale inviato da Spark
                    if isinstance(kafka_envelope, dict) and "value" in kafka_envelope:
                        # Se 'value' è a sua volta una stringa JSON, la decodifichiamo
                        if isinstance(kafka_envelope["value"], str):
                            actual_record = json.loads(kafka_envelope["value"])
                        else:
                            actual_record = kafka_envelope["value"]
                    else:
                        actual_record = kafka_envelope
                    
                    # Debug di verifica campi
                    if "mesh_condition_id" in actual_record:
                        buffer.append(actual_record)
                    else:
                        print(f"[WARN]: Record decodificato ma manca la chiave attesa. Record: {actual_record}")
                        
                except Exception as parse_err:
                    print(f"[ERR PARSING]: Fallito il parsing del singolo messaggio: {parse_err}")

            # 3. Flusshiamo il batch pieno solo SE abbiamo accumulato record validi nel buffer
            if buffer:
                print(f"[BATCH]: Invio un blocco di {len(buffer)} record al database...")
                flush_buffer(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Consumer arrestato manualmente.")
    except Exception as e:
        print(f"[CRITICAL ERR]: Crash del main loop: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()