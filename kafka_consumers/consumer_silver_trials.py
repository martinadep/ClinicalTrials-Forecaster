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
KAFKA_TOPIC_SILVER_TRIALS = os.getenv("KAFKA_TOPIC_SILVER_TRIALS", "kt.silver.trials")


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
            enrollment_duration_months, trial_velocity, phase, mesh_conditions_ids,
            has_non_diagnostic_condition, transformed_at
        ) VALUES %s
        ON CONFLICT (nct_id) DO UPDATE SET
            brief_title = EXCLUDED.brief_title, brief_summary = EXCLUDED.brief_summary,
            study_type = EXCLUDED.study_type, primary_purpose = EXCLUDED.primary_purpose,
            overall_status = EXCLUDED.overall_status, lead_sponsor_class = EXCLUDED.lead_sponsor_class,
            enrollment_count = EXCLUDED.enrollment_count, start_date = EXCLUDED.start_date,
            primary_completion_date = EXCLUDED.primary_completion_date, sex = EXCLUDED.sex,
            minimum_age_years = EXCLUDED.minimum_age_years, maximum_age_years = EXCLUDED.maximum_age_years,
            enrollment_duration_months = EXCLUDED.enrollment_duration_months, trial_velocity = EXCLUDED.trial_velocity,
            phase = EXCLUDED.phase, mesh_conditions_ids = EXCLUDED.mesh_conditions_ids,
            has_non_diagnostic_condition = EXCLUDED.has_non_diagnostic_condition, transformed_at = EXCLUDED.transformed_at
        """,
        [
            (
                r.get("nct_id"), 
                r.get("brief_title"), 
                r.get("brief_summary"), 
                r.get("study_type"), 
                r.get("primary_purpose"), 
                r.get("overall_status"), 
                r.get("lead_sponsor_class"), 
                r.get("enrollment_count"), 
                r.get("start_date"),
                r.get("primary_completion_date"), 
                r.get("sex"), 
                r.get("minimum_age_years"), 
                r.get("maximum_age_years"), 
                r.get("enrollment_duration_months"), 
                r.get("trial_velocity"), 
                r.get("phase"), 
                r.get("mesh_conditions_ids"),
                r.get("has_non_diagnostic_condition"),
                now_ts
            )
            for r in records
        ],
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    )

def flush_buffer(records):
    if not records: return
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                save_trials(cur, records)
        print(f"[INFO DB - TRIALS]: Successfully uploaded batch of {len(records)} clinical trial records.")
    except Exception as e:
        print(f"[ERR DB - TRIALS]: Error encountered during batch flush sequence: {e}")
        raise e
    finally:
        conn.close()

def main():
    consumer = get_kafka_consumer()
    consumer.subscribe([KAFKA_TOPIC_SILVER_TRIALS])
    print(f"[START]: TRIALS consumer node active on topic: {KAFKA_TOPIC_SILVER_TRIALS}")

    BATCH_SIZE = 500
    TIMEOUT = 3.0
    buffer = []

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=TIMEOUT)
            
            if not messages:
                if buffer:
                    print(f"[TIMEOUT]: Flushing remaining residual trial data buffer ({len(buffer)} records)...")
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
                    print(f"[ERR PARSING TRIALS]: Parsing routine failure: {parse_err}")

            if buffer:
                print(f"[BATCH]: Transferring {len(buffer)} trial blocks to internal relational database...")
                flush_buffer(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()

    except KeyboardInterrupt:
        print("[STOP]: Trials loader consumer halted cleanly by request.")
    except Exception as e:
        print(f"[CRITICAL ERR TRIALS]: Consumer loop crashed unexpected context: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()