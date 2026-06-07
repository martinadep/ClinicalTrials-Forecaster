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

# import json
# import hashlib
# import time
# import os
# import datetime

# import requests
# from confluent_kafka import Producer

# API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
# RETRIES = 5
# SLEEP_RETRY = 2
# SAMPLE_SIZE = 1

# def _build_kafka_producer():
#     """Create a Kafka producer from environment config, or None if not configured."""
#     broker = os.getenv("KAFKA_BROKER")
#     if not broker:
#         print("[INFO]: KAFKA_BROKER not set; skipping Kafka production.")
#         return None
#     return Producer({"bootstrap.servers": broker})


# def _delivery_report(err, msg):
#     """Callback called once for each produced message to confirm delivery."""
#     if err is not None:
#         print(f"[ERR]: Kafka delivery failed: {err}")
#     else:
#         print(f"[INFO]: Produced to {msg.topic()} partition {msg.partition()}")

# def _produce_study_to_kafka(producer, study):
#     """Produce a single study payload to the bronze topic."""
#     if producer is None:
#         return
#     topic = os.getenv("KAFKA_TOPIC_BRONZE", "trials.bronze")
#     nct_id = (
#         study.get("protocolSection", {})
#         .get("identificationModule", {})
#         .get("nctId")
#     )
#     payload = json.dumps(study, ensure_ascii=False)
#     producer.produce(
#         topic,
#         key=nct_id.encode("utf-8") if nct_id else None,
#         value=payload.encode("utf-8"),
#         callback=_delivery_report,
#     )
#     producer.poll(0)  # triggers delivery callbacks

# def _load_dotenv(dotenv_path=None):
#     """Load simple KEY=VALUE pairs from a local .env file into os.environ."""
#     if dotenv_path is None:
#         project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
#         dotenv_path = os.path.join(project_root, ".env")

#     if not os.path.exists(dotenv_path):
#         return

#     with open(dotenv_path, "r", encoding="utf-8") as env_file:
#         for line in env_file:
#             stripped = line.strip()
#             if not stripped or stripped.startswith("#") or "=" not in stripped:
#                 continue
#             key, value = stripped.split("=", 1)
#             os.environ.setdefault(key.strip(), value.strip())


# def _stable_payload_hash(study):
#     normalized = json.dumps(study, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
#     return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# def _build_dsn_from_env():
#     user = os.getenv("POSTGRES_USER")
#     password = os.getenv("POSTGRES_PASSWORD")
#     db = os.getenv("POSTGRES_DB")
#     port = os.getenv("POSTGRES_PORT", "5432")
#     host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or "localhost"
#     if user and password and db:
#         return f"postgresql://{user}:{password}@{host}:{port}/{db}"
#     return None


# def fetch_clinical_trial(page_token=None):
#     params = {}
#     if page_token:
#         params["pageToken"] = page_token

#     for attempt in range(RETRIES):
#         try:
#             response = requests.get(API_BASE_URL, params=params, timeout=60)
#             if response.status_code == 200:
#                 data = response.json()
#                 return data.get("studies", []), data.get("nextPageToken")
#             print(f"[ERR]: {response.status_code}")
#         except requests.exceptions.RequestException as e:
#             print(f"[ERR]: An error occurred {e}")
#             if attempt < RETRIES - 1:
#                 time.sleep(SLEEP_RETRY)
#     return None


# def _parse_date(date_str):
#     """Normalize date strings into YYYY-MM-DD or None."""
#     if not date_str:
#         return None
#     if isinstance(date_str, dict):
#         date_str = date_str.get("date") or date_str.get("value") or None
#     if not isinstance(date_str, str):
#         return None
#     s = date_str.strip()
#     try:
#         if len(s) == 10:
#             datetime.date.fromisoformat(s)
#             return s
#         if len(s) == 7:
#             return s + "-01"
#         if len(s) == 4:
#             return s + "-01-01"
#         dt = datetime.date.fromisoformat(s)
#         return dt.isoformat()
#     except Exception:
#         return None


# def _extract_trial_fields(study):
#     """Extract normalized fields used by bronze tables from one study payload."""
#     protocol = study.get("protocolSection", {})
#     identification = protocol.get("identificationModule", {})
#     status = protocol.get("statusModule", {})
#     design = protocol.get("designModule", {})
#     sponsor = protocol.get("sponsorCollaboratorsModule", {})
#     eligibility = protocol.get("eligibilityModule", {})
#     derived = study.get("derivedSection", {})

#     design_info = design.get("designInfo", {})
#     enrollment = design.get("enrollmentInfo", {})

#     return {
#         "nct_id": identification.get("nctId"),
#         "payload_hash": _stable_payload_hash(study),
#         "brief_title": identification.get("briefTitle"),
#         "official_title": identification.get("officialTitle"),
#         "acronym": identification.get("acronym"),
#         "conditions": protocol.get("conditionsModule", {}).get("conditions"),
#         "keywords": protocol.get("conditionsModule", {}).get("keywords"),
#         "study_type": design.get("studyType"),
#         "phases": design.get("phases"),
#         "allocation": design_info.get("allocation"),
#         "intervention_model": design_info.get("interventionModel"),
#         "primary_purpose": design_info.get("primaryPurpose"),
#         "enrollment_count": enrollment.get("count"),
#         "enrollment_type": enrollment.get("type"),
#         "overall_status": status.get("overallStatus"),
#         "start_date": _parse_date(status.get("startDateStruct", {}).get("date") if status.get("startDateStruct") else status.get("startDateStruct")),
#         "primary_completion_date": _parse_date(status.get("primaryCompletionDateStruct", {}).get("date") if status.get("primaryCompletionDateStruct") else None),
#         "completion_date": _parse_date(status.get("completionDateStruct", {}).get("date") if status.get("completionDateStruct") else None),
#         "study_first_post_date": _parse_date(status.get("studyFirstPostDateStruct", {}).get("date") if status.get("studyFirstPostDateStruct") else None),
#         "last_update_post_date": _parse_date(status.get("lastUpdatePostDateStruct", {}).get("date") if status.get("lastUpdatePostDateStruct") else None),
#         "lead_sponsor": sponsor.get("leadSponsor"),
#         "organization_class": identification.get("organization", {}).get("class") if identification.get("organization") else None,
#         "responsible_party": sponsor.get("responsibleParty"),
#         "eligibility_criteria": eligibility.get("eligibilityCriteria"),
#         "healthy_volunteers": eligibility.get("healthyVolunteers"),
#         "sex": eligibility.get("sex"),
#         "minimum_age": eligibility.get("minimumAge"),
#         "maximum_age": eligibility.get("maximumAge"),
#         "locations": protocol.get("contactsLocationsModule", {}).get("locations"),
#         "version_holder": derived.get("miscInfoModule", {}).get("versionHolder") if derived.get("miscInfoModule") else None,
#     }


# def _upsert_raw_trial(cur, fields, raw_payload):
#     """Insert or update bronze.raw_trials row and return raw_id."""
#     nct_id = fields["nct_id"]
#     payload_hash = fields["payload_hash"]

#     existing_raw = None
#     if nct_id:
#         cur.execute(
#             "SELECT id, payload_hash FROM bronze.raw_trials WHERE nct_id = %s LIMIT 1",
#             (nct_id,),
#         )
#         existing_raw = cur.fetchone()
        
#     if existing_raw:
#         raw_id, existing_raw_hash = existing_raw
#         if existing_raw_hash != payload_hash:
#             cur.execute(
#                 """
#                 UPDATE bronze.raw_trials
#                 SET nct_id = %s, payload = %s, payload_hash = %s, updated_at = NOW()
#                 WHERE id = %s
#                 """,
#                 (nct_id, raw_payload, payload_hash, raw_id),
#             )
#         print(f"[INFO]: Updated raw trial with id {raw_id} for nct_id {nct_id}")
#         return raw_id

#     cur.execute(
#         """
#         INSERT INTO bronze.raw_trials (nct_id, payload_hash, payload)
#         VALUES (%s, %s, %s)
#         RETURNING id
#         """,
#         (nct_id, payload_hash, raw_payload),
#     )
#     raw_id = cur.fetchone()[0]
#     print(f"[INFO]: Inserted raw trial with id {raw_id} for nct_id {nct_id}")
#     return raw_id


# def _build_trial_values(raw_id, fields, psycopg2_extras):
#     """Build positional values tuple for bronze.trials write operations."""
#     return (
#         raw_id,
#         fields["nct_id"],
#         fields["payload_hash"],
#         fields["brief_title"],
#         fields["official_title"],
#         fields["acronym"],
#         psycopg2_extras.Json(fields["conditions"]) if fields["conditions"] is not None else None,
#         psycopg2_extras.Json(fields["keywords"]) if fields["keywords"] is not None else None,
#         fields["study_type"],
#         fields["phases"],
#         fields["allocation"],
#         fields["intervention_model"],
#         fields["primary_purpose"],
#         fields["enrollment_count"],
#         fields["enrollment_type"],
#         fields["overall_status"],
#         fields["start_date"],
#         fields["primary_completion_date"],
#         fields["completion_date"],
#         fields["study_first_post_date"],
#         fields["last_update_post_date"],
#         psycopg2_extras.Json(fields["lead_sponsor"]) if fields["lead_sponsor"] else None,
#         fields["organization_class"],
#         psycopg2_extras.Json(fields["responsible_party"]) if fields["responsible_party"] else None,
#         fields["eligibility_criteria"],
#         fields["healthy_volunteers"],
#         fields["sex"],
#         fields["minimum_age"],
#         fields["maximum_age"],
#         psycopg2_extras.Json(fields["locations"]) if fields["locations"] else None,
#         fields["version_holder"],
#     )


# def _upsert_trial_row(cur, trial_values, nct_id, payload_hash):
#     """Insert or update bronze.trials row based on nct_id and payload_hash."""
#     existing_trial = None
#     if nct_id:
#         cur.execute("SELECT id, payload_hash FROM bronze.trials WHERE nct_id = %s LIMIT 1", (nct_id,))
#         existing_trial = cur.fetchone()

#     if existing_trial:
#         existing_id, existing_trial_hash = existing_trial
#         if existing_trial_hash != payload_hash:
#             cur.execute(
#                 """
#                 UPDATE bronze.trials
#                 SET
#                     raw_id = %s,
#                     nct_id = %s,
#                     payload_hash = %s,
#                     brief_title = %s,
#                     official_title = %s,
#                     acronym = %s,
#                     conditions = %s,
#                     keywords = %s,
#                     study_type = %s,
#                     phases = %s,
#                     allocation = %s,
#                     intervention_model = %s,
#                     primary_purpose = %s,
#                     enrollment_count = %s,
#                     enrollment_type = %s,
#                     overall_status = %s,
#                     start_date = %s,
#                     primary_completion_date = %s,
#                     completion_date = %s,
#                     study_first_post_date = %s,
#                     last_update_post_date = %s,
#                     lead_sponsor = %s,
#                     organization_class = %s,
#                     responsible_party = %s,
#                     eligibility_criteria = %s,
#                     healthy_volunteers = %s,
#                     sex = %s,
#                     minimum_age = %s,
#                     maximum_age = %s,
#                     locations = %s,
#                     version_holder = %s,
#                     updated_at = NOW()
#                 WHERE id = %s
#                 """,
#                 trial_values + (existing_id,),
#             )
#         return

#     cur.execute(
#         """
#         INSERT INTO bronze.trials (
#             raw_id, nct_id, payload_hash, brief_title, official_title, acronym,
#             conditions, keywords, study_type, phases, allocation,
#             intervention_model, primary_purpose, enrollment_count, enrollment_type,
#             overall_status, start_date, primary_completion_date, completion_date,
#             study_first_post_date, last_update_post_date, lead_sponsor, organization_class,
#             responsible_party, eligibility_criteria, healthy_volunteers, sex, minimum_age,
#             maximum_age, locations, version_holder
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """,
#         trial_values,
#     )


# def insert_study_into_db(study, dsn=None):
#     """Insert or update a study in bronze raw/current tables."""
#     try:
#         import psycopg2
#         import psycopg2.extras
#     except Exception as e:
#         raise RuntimeError("psycopg2 is required to insert into the database: %s" % e)

#     dsn = dsn or os.getenv("DATABASE_URL")
#     if not dsn:
#         raise ValueError("No DSN provided and DATABASE_URL not set")

#     fields = _extract_trial_fields(study)
#     raw_payload = psycopg2.extras.Json(study)

#     conn = psycopg2.connect(dsn)
#     try:
#         with conn:
#             with conn.cursor() as cur:
#                 raw_id = _upsert_raw_trial(cur, fields, raw_payload)
#                 trial_values = _build_trial_values(raw_id, fields, psycopg2.extras)
#                 _upsert_trial_row(cur, trial_values, fields["nct_id"], fields["payload_hash"])
#                 return raw_id
#     finally:
#         conn.close()


# def _write_study_to_file(study, fallback_id):
#     sid = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId") or fallback_id
#     with open(f"sample_study_{sid}.json", "w", encoding="utf-8") as f:
#         f.write(json.dumps(study, indent=2))
#     print(f"[INFO]: Wrote study {sid} to sample_study_{sid}.json")


# def process_page(study_list, dsn, producer=None):
#     inserted = 0
#     for idx, study in enumerate(study_list):
#         try:
#             if dsn:
#                 raw_id = insert_study_into_db(study, dsn=dsn)
#                 nct_id = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
#                 _produce_study_to_kafka(producer, study)
#             else:
#                 _write_study_to_file(study, idx)
#             inserted += 1
#         except Exception as e:
#             print(f"[ERR]: Failed inserting study: {e}")
#     return inserted


# def main():
#     _load_dotenv()

#     next_page_token = None
#     dsn = os.getenv("DATABASE_URL") or _build_dsn_from_env()
#     producer = _build_kafka_producer()

#     if dsn:
#         print(
#             f"[INFO]: Using database connection from environment for {os.getenv('POSTGRES_DB', 'clinical_trials')} "
#             f"at {os.getenv('POSTGRES_HOST') or os.getenv('DB_HOST') or 'localhost'}:{os.getenv('POSTGRES_PORT', '5432')}"
#         )
#     else:
#         print("[INFO]: No database DSN found; inserts will be skipped and studies will be written to JSON files instead.")

#     for i in range(SAMPLE_SIZE):
#         print(f"[INFO]: Fetching page {i + 1} / {SAMPLE_SIZE} of clinical trials data...")
#         result = fetch_clinical_trial(next_page_token)
#         if not result:
#             print("[ERR]: Error fetching clinical trials data.")
#             break

#         study_list, next_token = result
#         inserted = process_page(study_list, dsn, producer=producer)
#         print(f"[INFO]: Processed {len(study_list)} studies, attempted inserts: {inserted}")

#         next_page_token = next_token
#         if not next_page_token:
#             break

#     if producer is not None:
#         producer.flush()
#         print("[INFO]: Flushed all Kafka messages.")


# if __name__ == "__main__":
#     main()