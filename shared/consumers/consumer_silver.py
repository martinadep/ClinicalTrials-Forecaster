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
TOPIC_SILVER_TRIALS = "trials.silver"
TOPIC_SILVER_SITES = "sites.silver"
TOPIC_GOLD_MESH = "mesh.gold"

def get_kafka_consumer():
    broker = os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": broker,
        "group.id": "clinical_trials_silver_relational_loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)

def save_trials(cur, records):
    now_ts = datetime.datetime.now()
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO silver.trials (
            nct_id, brief_title, brief_summary, study_type, primary_purpose,
            overall_status, lead_sponsor_class, enrollment_count, start_date,
            primary_completion_date, sex, minimum_age_years, maximum_age_years, 
            enrollment_duration_months, trial_velocity, phase, mesh_conditions_ids, transformed_at
        ) VALUES %s
        ON CONFLICT (nct_id) DO UPDATE SET
            brief_title = EXCLUDED.brief_title, brief_summary = EXCLUDED.brief_summary,
            study_type = EXCLUDED.study_type, primary_purpose = EXCLUDED.primary_purpose,
            overall_status = EXCLUDED.overall_status, lead_sponsor_class = EXCLUDED.lead_sponsor_class,
            enrollment_count = EXCLUDED.enrollment_count, start_date = EXCLUDED.start_date,
            primary_completion_date = EXCLUDED.primary_completion_date, sex = EXCLUDED.sex,
            minimum_age_years = EXCLUDED.minimum_age_years, maximum_age_years = EXCLUDED.maximum_age_years,
            enrollment_duration_months = EXCLUDED.enrollment_duration_months, trial_velocity = EXCLUDED.trial_velocity,
            phase = EXCLUDED.phase, mesh_conditions_ids = EXCLUDED.mesh_conditions_ids, transformed_at = EXCLUDED.transformed_at
        """,
        [
            (
                r["nct_id"], r["brief_title"], r["brief_summary"], r["study_type"], r["primary_purpose"],
                r["overall_status"], r["lead_sponsor_class"], r["enrollment_count"], r["start_date"],
                r["primary_completion_date"], r["sex"], r["minimum_age_years"], r["maximum_age_years"], 
                r["enrollment_duration_months"], r["trial_velocity"], r["phase"], r["mesh_conditions_ids"],
                now_ts
            )
            for r in records
        ],
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    )
    print(f"[INFO DB]: Caricati {len(records)} record in silver.trials.")

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
    print(f"[INFO DB]: Sincronizzati {len(records)} record in silver.trial_sites.")

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
    print(f"[INFO DB]: Aggiornate {len(records)} dimensioni in gold.dim_mesh_conditions.")

def flush_all_buffers(buffer):
    """Esegue lo scaricamento garantendo atomicità completa: o tutto o niente."""
    if not buffer:
        return

    conn = psycopg2.connect(DSN)
    try:
        with conn: # Gestisce il commit/rollback dell'intero pacchetto multi-tabella
            with conn.cursor() as cur:
                if TOPIC_SILVER_TRIALS in buffer:
                    save_trials(cur, buffer[TOPIC_SILVER_TRIALS])
                if TOPIC_SILVER_SITES in buffer:
                    save_sites(cur, buffer[TOPIC_SILVER_SITES])
                if TOPIC_GOLD_MESH in buffer:
                    save_mesh(cur, buffer[TOPIC_GOLD_MESH])
    except Exception as e:
        print(f"[ERR DB - SILVER]: Svuotamento buffer relazionale fallito atomica: {e}")
        raise e
    finally:
        conn.close()

def main():
    consumer = get_kafka_consumer()
    target_topics = [TOPIC_SILVER_TRIALS, TOPIC_SILVER_SITES, TOPIC_GOLD_MESH]
    consumer.subscribe(target_topics)
    print(f"[START]: Consumer SILVER attivo sui canali trasformati: {target_topics}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = {}

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            if not messages:
                if buffer:
                    flush_all_buffers(buffer)
                    consumer.commit(asynchronous=False)
                    print(f"[TIMEOUT]: Flush dei dati residui completato con successo.")
                    buffer.clear()
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF: 
                        continue
                    else: 
                        raise KafkaException(msg.error())
                
                topic = msg.topic()
                payload = json.loads(msg.value().decode("utf-8"))
                buffer.setdefault(topic, []).append(payload)

            if buffer:
                flush_all_buffers(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Consumer Silver arrestato manualmente.")
    except Exception as e:
        print(f"[CRITICAL ERR]: Crash del consumer relazionale per protezione dati. Errore: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()