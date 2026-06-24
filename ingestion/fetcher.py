import os
import time
import datetime
import requests
import json
import argparse  

from shared.config import load_dotenv
from shared.kafka import build_kafka_producer, produce_study_to_kafka

API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
RETRIES = 5
SLEEP_RETRY = 2
PAGE_SIZE = 1000
DEFAULT_MAX_TRIALS = 15000  

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

def process_page(study_list, producer):
    inserted = 0
    for study in study_list:
        try:
            produce_study_to_kafka(producer, study)
            inserted += 1
        except Exception as e:
            print(f"[ERR]: Failed inserting study: {e}")
    return inserted

def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Clinical Trials Fetcher")
    parser.add_argument(
        "--max-trials", 
        type=int, 
        default=int(os.getenv("MAX_TRIALS", DEFAULT_MAX_TRIALS)),
        help="Maximum number of trials to fetch (ignored in DAILY run mode)"
    )
    args = parser.parse_args()
    
    max_trials = args.max_trials
    is_daily_run = os.getenv("RUN_MODE", "FULL").upper() == "DAILY"
    
    if not is_daily_run:
        print(f"[INFO]: Dynamic MAX_TRIALS set to: {max_trials}")
        
    next_page_token = None
    producer = build_kafka_producer()

    if producer is None:
        print("[ERR]: Could not initialize Kafka Producer. Aborting.")
        return

    page_counter = 1
    total_processed = 0

    while True:
        result = fetch_clinical_trial(next_page_token, is_daily_run=is_daily_run)
        if result is None: break

        study_list, next_token = result
        if not study_list: break

        inserted = process_page(study_list, producer)
        total_processed += inserted
        print(f"[INFO]: Page {page_counter} completed. Processed {len(study_list)} trials. Total: {total_processed}")

        if not is_daily_run and total_processed >= max_trials:
            print(f"[INFO]: Reached max trials limit ({max_trials}). Stopping.")
            break

        next_page_token = next_token
        page_counter += 1
        if not next_page_token: break
        time.sleep(0.5)

    if producer is not None:
        producer.flush()

if __name__ == "__main__":
    main()