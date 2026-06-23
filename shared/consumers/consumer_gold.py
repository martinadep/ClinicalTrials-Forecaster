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
TOPIC_GOLD_FEATURES = os.getenv("KAFKA_TOPIC_GOLD_FEATURES", "trials.gold")

def get_kafka_consumer():
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": broker,
        "group.id": "clinical_trials_gold_features_loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)

def save_trial_features(cur, records):
    if not records:
        return

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO gold.trial_features (
            nct_id, study_type, primary_purpose, lead_sponsor_class, sex,
            phase, enrollment_count, n_sites, duration_months, 
            mesh_conditions_ids, avg_site_exp, avg_site_vel, target_velocity
        ) VALUES %s
        ON CONFLICT (nct_id) DO UPDATE SET
            study_type = EXCLUDED.study_type,
            primary_purpose = EXCLUDED.primary_purpose,
            lead_sponsor_class = EXCLUDED.lead_sponsor_class,
            sex = EXCLUDED.sex,
            phase = EXCLUDED.phase,
            enrollment_count = EXCLUDED.enrollment_count,
            n_sites = EXCLUDED.n_sites,
            duration_months = EXCLUDED.duration_months,
            mesh_conditions_ids = EXCLUDED.mesh_conditions_ids,
            avg_site_exp = EXCLUDED.avg_site_exp,
            avg_site_vel = EXCLUDED.avg_site_vel,
            target_velocity = EXCLUDED.target_velocity
        """,
        [
            (
                r["nct_id"], r.get("study_type"), r.get("primary_purpose"), r.get("lead_sponsor_class"), r.get("sex"),
                r.get("phase"), r.get("enrollment_count"), r.get("n_sites"), r.get("duration_months"),
                r.get("mesh_conditions_ids", []), r.get("avg_site_exp"), r.get("avg_site_vel"), r.get("target_velocity")
            )
            for r in records
        ],
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    )
    print(f"[INFO DB]: Upsert completato per {len(records)} righe in gold.trial_features.")

def flush_buffer(records):
    if not records:
        return
    
    conn = psycopg2.connect(DSN)
    try:
        # Sfruttiamo appieno il context manager di psycopg2 che apre e chiude la transazione automaticamente
        with conn:
            with conn.cursor() as cur:
                save_trial_features(cur, records)
    except Exception as e:
        print(f"[ERR DB - GOLD]: Errore durante il caricamento delle feature: {e}")
        raise e
    finally:
        conn.close()

def main():
    consumer = get_kafka_consumer()
    consumer.subscribe([TOPIC_GOLD_FEATURES])
    print(f"[START]: Consumer GOLD attivo in ascolto sulle feature in streaming: {TOPIC_GOLD_FEATURES}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = []

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            if not messages:
                if buffer:
                    flush_buffer(buffer)
                    consumer.commit(asynchronous=False)
                    print(f"[TIMEOUT]: Svuotato buffer residuo delle feature ({len(buffer)} record).")
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
                flush_buffer(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Consumer Gold interrotto dall'utente.")
    except Exception as e:
        print(f"[CRITICAL ERR]: Crash irreversibile nel consumer Gold: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()