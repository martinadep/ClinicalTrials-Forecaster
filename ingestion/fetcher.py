import json
import time
import os
import requests

from shared.config import load_dotenv
from shared.db import build_dsn_from_env, insert_study_into_db
from shared.kafka import build_kafka_producer, produce_study_to_kafka

API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
RETRIES = 5
SLEEP_RETRY = 2
SAMPLE_SIZE = 1


def fetch_clinical_trial(page_token=None):
    params = {}
    if page_token:
        params["pageToken"] = page_token

    for attempt in range(RETRIES):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get("studies", []), data.get("nextPageToken")
            print(f"[ERR]: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[ERR]: An error occurred {e}")
            if attempt < RETRIES - 1:
                time.sleep(SLEEP_RETRY)
    return None


def _write_study_to_file(study, fallback_id):
    sid = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId") or fallback_id
    with open(f"sample_study_{sid}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(study, indent=2))
    print(f"[INFO]: Wrote study {sid} to sample_study_{sid}.json")


def process_page(study_list, dsn, producer=None):
    inserted = 0
    for idx, study in enumerate(study_list):
        try:
            if dsn:
                # 1. Write batch data directly into DB
                insert_study_into_db(study, dsn=dsn)
                # 2. Mirror/buffer to Kafka queue
                produce_study_to_kafka(producer, study)
            else:
                _write_study_to_file(study, idx)
            inserted += 1
        except Exception as e:
            print(f"[ERR]: Failed inserting study: {e}")
    return inserted


def main():
    load_dotenv()

    next_page_token = None
    dsn = os.getenv("DATABASE_URL") or build_dsn_from_env()
    producer = build_kafka_producer()

    if dsn:
        print(
            f"[INFO]: Using database connection from environment for {os.getenv('POSTGRES_DB', 'clinical_trials')} "
            f"at {os.getenv('POSTGRES_HOST') or os.getenv('DB_HOST') or 'localhost'}:{os.getenv('POSTGRES_PORT', '5432')}"
        )
    else:
        print("[INFO]: No database DSN found; inserts will be skipped and studies will be written to JSON files instead.")

    for i in range(SAMPLE_SIZE):
        print(f"[INFO]: Fetching page {i + 1} / {SAMPLE_SIZE} of clinical trials data...")
        result = fetch_clinical_trial(next_page_token)
        if not result:
            print("[ERR]: Error fetching clinical trials data.")
            break

        study_list, next_token = result
        inserted = process_page(study_list, dsn, producer=producer)
        print(f"[INFO]: Processed {len(study_list)} studies, attempted inserts: {inserted}")

        next_page_token = next_token
        if not next_page_token:
            break

    if producer is not None:
        producer.flush()
        print("[INFO]: Flushed all Kafka messages.")


if __name__ == "__main__":
    main()
