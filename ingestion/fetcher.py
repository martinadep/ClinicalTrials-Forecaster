import json
import time
import os
import requests
import datetime

from shared.config import load_dotenv
from shared.db import build_dsn_from_env, insert_study_into_db
from shared.kafka import build_kafka_producer, produce_study_to_kafka

API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
RETRIES = 5
SLEEP_RETRY = 2
PAGE_SIZE = 1000
MAX_TRIALS = 15000

def fetch_clinical_trial(page_token=None, is_daily_run=False):
    params = {
        "pageSize": PAGE_SIZE,    
        "filter.overallStatus": "COMPLETED"  
    }
    
    if page_token:
        params["pageToken"] = page_token

    if is_daily_run:
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        params["filter.advanced"] = f"LAST_UPDATE_DATE >= {yesterday}"
        print(f"[FETCH]: Filtering on LAST_UPDATE_DATE >= {yesterday}")

    for attempt in range(RETRIES):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get("studies", []), data.get("nextPageToken")
            print(f"[ERR]: Status Code {response.status_code} on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"[ERR]: Network Error: {e}")
            if attempt < RETRIES - 1:
                time.sleep(SLEEP_RETRY)
    return None


def _write_study_to_file(study, fallback_id):
    sid = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId") or fallback_id
    with open(f"sample_study_{sid}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(study, indent=2))


def process_page(study_list, dsn, producer=None):
    inserted = 0
    for idx, study in enumerate(study_list):
        try:
            if dsn:
                insert_study_into_db(study, dsn=dsn)
                produce_study_to_kafka(producer, study)
            else:
                _write_study_to_file(study, idx)
            inserted += 1
        except Exception as e:
            print(f"[ERR]: Failed inserting study: {e}")
    return inserted


def main():
    load_dotenv()

    is_daily_run = os.getenv("RUN_MODE", "FULL").upper() == "DAILY"

    next_page_token = None
    dsn = os.getenv("DATABASE_URL") or build_dsn_from_env()
    producer = build_kafka_producer()

    if dsn:
        print(f"[INFO]: Connected to Postgres Database. Starting ingestion...")
    else:
        # print("[INFO]: No database found. Writing to local JSON files.")
        print("Could not connect to Postgres Database. Aborting.")
        return

    page_counter = 1
    total_processed = 0

    while True:
        print(f"[INFO]: Starting Fetch Page {page_counter} (Token: {next_page_token or 'Initial'})...")
        
        result = fetch_clinical_trial(next_page_token, is_daily_run=is_daily_run)
        if result is None:
            print("[ERR]: Error fetching data from ClinicalTrials.gov API. Aborting.")
            break

        study_list, next_token = result
        
        if not study_list:
            print("[INFO]: No more trials to fetch.")
            break

        inserted = process_page(study_list, dsn, producer=producer)
        total_processed += inserted
        print(f"[INFO]: Page {page_counter} completed. Processed {len(study_list)} trials. Total progress: {total_processed}")

        if not is_daily_run and total_processed >= MAX_TRIALS:
            print(f"[INFO]: Reached the predefined limit of significant samples ({total_processed} >= {MAX_TRIALS}). Stop Ingestion.")
            break

        next_page_token = next_token
        page_counter += 1

        if not next_page_token:
            print("[INFO]: Reached last page.")
            break

        time.sleep(0.5)

    if producer is not None:
        producer.flush()
        print(f"[INFO]: Pipeline completed successfully, trials processed: {total_processed}")


if __name__ == "__main__":
    main()