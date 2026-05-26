import json
import hashlib
import time
import os
import datetime

import requests


API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
RETRIES = 5
SLEEP_RETRY = 2
SAMPLE_SIZE = 1


def _load_dotenv(dotenv_path=None):
    """Load simple KEY=VALUE pairs from a local .env file into os.environ.

    This keeps the script self-contained without requiring python-dotenv.
    Existing environment variables are not overwritten.
    """
    if dotenv_path is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        dotenv_path = os.path.join(project_root, ".env")

    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()


def _stable_payload_hash(study):
    normalized = json.dumps(study, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

def fetch_clinical_trial(page_token=None):
    params = {}
    if page_token:
        params["pageToken"] = page_token

    for attempt in range(RETRIES):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json() # data is a dict with keys "studies" (list) and "nextPageToken"
                return data.get("studies", []), data.get("nextPageToken")
            else:
                print(f"Error: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            if attempt < RETRIES - 1:
                time.sleep(SLEEP_RETRY)  
    return None


def _parse_date(date_str):
    """Normalize various date string formats from the payload into YYYY-MM-DD or None.
    Accepts 'YYYY-MM-DD', 'YYYY-MM' or 'YYYY' and returns a string suitable for
    inserting into a Postgres DATE column.
    """
    if not date_str:
        return None
    if isinstance(date_str, dict):
        date_str = date_str.get("date") or date_str.get("value") or None
    if not isinstance(date_str, str):
        return None
    s = date_str.strip()
    try:
        if len(s) == 10:
            # YYYY-MM-DD
            datetime.date.fromisoformat(s)
            return s
        if len(s) == 7:
            # YYYY-MM -> use first day of month
            return s + "-01"
        if len(s) == 4:
            # Year only
            return s + "-01-01"
        # fallback: try to parse
        dt = datetime.date.fromisoformat(s)
        return dt.isoformat()
    except Exception:
        return None


def insert_study_into_db(study, dsn=None):
    """Insert or update a study in `bronze.raw_trials` and `bronze.trials`.

    Existing rows are only updated when the payload hash changes.
    Returns the affected raw_id.
    """
    try:
        import psycopg2
        import psycopg2.extras
    except Exception as e:
        raise RuntimeError("psycopg2 is required to insert into the database: %s" % e)

    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("No DSN provided and DATABASE_URL not set")

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                protocol = study.get("protocolSection", {})
                identification = protocol.get("identificationModule", {})
                status = protocol.get("statusModule", {})
                design = protocol.get("designModule", {})
                sponsor = protocol.get("sponsorCollaboratorsModule", {})
                eligibility = protocol.get("eligibilityModule", {})
                derived = study.get("derivedSection", {})

                nct_id = identification.get("nctId")
                raw_payload = psycopg2.extras.Json(study)
                raw_id = None

                brief_title = identification.get("briefTitle")
                official_title = identification.get("officialTitle")
                acronym = identification.get("acronym")
                conditions = protocol.get("conditionsModule", {}).get("conditions")
                keywords = protocol.get("conditionsModule", {}).get("keywords")

                study_type = design.get("studyType")
                phases = design.get("phases")
                design_info = design.get("designInfo", {})
                allocation = design_info.get("allocation")
                intervention_model = design_info.get("interventionModel")
                primary_purpose = design_info.get("primaryPurpose")

                enrollment = design.get("enrollmentInfo", {})
                enrollment_count = enrollment.get("count")
                enrollment_type = enrollment.get("type")

                overall_status = status.get("overallStatus")
                start_date = _parse_date(status.get("startDateStruct", {}).get("date") if status.get("startDateStruct") else status.get("startDateStruct"))
                primary_completion_date = _parse_date(status.get("primaryCompletionDateStruct", {}).get("date") if status.get("primaryCompletionDateStruct") else None)
                completion_date = _parse_date(status.get("completionDateStruct", {}).get("date") if status.get("completionDateStruct") else None)
                study_first_post_date = _parse_date(status.get("studyFirstPostDateStruct", {}).get("date") if status.get("studyFirstPostDateStruct") else None)
                last_update_post_date = _parse_date(status.get("lastUpdatePostDateStruct", {}).get("date") if status.get("lastUpdatePostDateStruct") else None)

                lead_sponsor = sponsor.get("leadSponsor")
                organization_class = identification.get("organization", {}).get("class") if identification.get("organization") else None
                responsible_party = sponsor.get("responsibleParty")

                eligibility_criteria = eligibility.get("eligibilityCriteria")
                healthy_volunteers = eligibility.get("healthyVolunteers")
                sex = eligibility.get("sex")
                minimum_age = eligibility.get("minimumAge")
                maximum_age = eligibility.get("maximumAge")

                locations = protocol.get("contactsLocationsModule", {}).get("locations")
                version_holder = derived.get("miscInfoModule", {}).get("versionHolder") if derived.get("miscInfoModule") else None
                payload_hash = _stable_payload_hash(study)

                existing_raw = None
                if nct_id:
                    cur.execute(
                        "SELECT id, payload_hash FROM bronze.raw_trials WHERE nct_id = %s LIMIT 1",
                        (nct_id,),
                    )
                    existing_raw = cur.fetchone()

                if existing_raw:
                    raw_id, existing_raw_hash = existing_raw
                    if existing_raw_hash != payload_hash:
                        cur.execute(
                            """
                            UPDATE bronze.raw_trials
                            SET nct_id = %s, payload = %s, payload_hash = %s, updated_at = NOW()
                            WHERE id = %s
                            """,
                            (nct_id, raw_payload, payload_hash, raw_id),
                        )
                else:
                    cur.execute(
                        """
                        INSERT INTO bronze.raw_trials (nct_id, payload_hash, payload)
                        VALUES (%s, %s, %s)
                        RETURNING id
                        """,
                        (nct_id, payload_hash, raw_payload),
                    )
                    raw_id = cur.fetchone()[0]

                trial_values = (
                    raw_id, nct_id, payload_hash, brief_title, official_title, acronym,
                    psycopg2.extras.Json(conditions) if conditions is not None else None,
                    psycopg2.extras.Json(keywords) if keywords is not None else None,
                    study_type, phases, allocation, intervention_model, primary_purpose,
                    enrollment_count, enrollment_type, overall_status, start_date,
                    primary_completion_date, completion_date, study_first_post_date,
                    last_update_post_date, psycopg2.extras.Json(lead_sponsor) if lead_sponsor else None,
                    organization_class, psycopg2.extras.Json(responsible_party) if responsible_party else None,
                    eligibility_criteria, healthy_volunteers, sex, minimum_age, maximum_age,
                    psycopg2.extras.Json(locations) if locations else None, version_holder
                )

                existing_trial = None
                if nct_id:
                    cur.execute("SELECT id, payload_hash FROM bronze.trials WHERE nct_id = %s LIMIT 1", (nct_id,))
                    existing_trial = cur.fetchone()

                if existing_trial:
                    existing_id, existing_trial_hash = existing_trial
                    if existing_trial_hash != payload_hash:
                        cur.execute(
                            """
                            UPDATE bronze.trials
                            SET
                                raw_id = %s,
                                nct_id = %s,
                                payload_hash = %s,
                                brief_title = %s,
                                official_title = %s,
                                acronym = %s,
                                conditions = %s,
                                keywords = %s,
                                study_type = %s,
                                phases = %s,
                                allocation = %s,
                                intervention_model = %s,
                                primary_purpose = %s,
                                enrollment_count = %s,
                                enrollment_type = %s,
                                overall_status = %s,
                                start_date = %s,
                                primary_completion_date = %s,
                                completion_date = %s,
                                study_first_post_date = %s,
                                last_update_post_date = %s,
                                lead_sponsor = %s,
                                organization_class = %s,
                                responsible_party = %s,
                                eligibility_criteria = %s,
                                healthy_volunteers = %s,
                                sex = %s,
                                minimum_age = %s,
                                maximum_age = %s,
                                locations = %s,
                                version_holder = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            trial_values + (existing_id,),
                        )
                else:
                    cur.execute(
                        """
                        INSERT INTO bronze.trials (
                            raw_id, nct_id, payload_hash, brief_title, official_title, acronym,
                            conditions, keywords, study_type, phases, allocation,
                            intervention_model, primary_purpose, enrollment_count, enrollment_type,
                            overall_status, start_date, primary_completion_date, completion_date,
                            study_first_post_date, last_update_post_date, lead_sponsor, organization_class,
                            responsible_party, eligibility_criteria, healthy_volunteers, sex, minimum_age,
                            maximum_age, locations, version_holder
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        trial_values,
                    )
                # transaction will commit on context exit
                return raw_id
    finally:
        conn.close()

def main():
    next_page_token = None

    # build DSN from env if DATABASE_URL not provided
    def _build_dsn_from_env():
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        port = os.getenv("POSTGRES_PORT", "5432")
        host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or "localhost"
        if user and password and db:
            return f"postgresql://{user}:{password}@{host}:{port}/{db}"
        return None

    dsn = os.getenv("DATABASE_URL") or _build_dsn_from_env()
    if dsn:
        print(f"Using database connection from environment for {os.getenv('POSTGRES_DB', 'clinical_trials')} at {os.getenv('POSTGRES_HOST') or os.getenv('DB_HOST') or 'localhost'}:{os.getenv('POSTGRES_PORT', '5432')}")
    else:
        print("No database DSN found; inserts will be skipped and studies will be written to JSON files instead.")

    for i in range(SAMPLE_SIZE):
        print(f"Fetching page {i+1} / {SAMPLE_SIZE} of clinical trials data...")
        result = fetch_clinical_trial(next_page_token)
        if not result:
            print("Error fetching clinical trials data.")
            break

        study_list, next_token = result
        inserted = 0
        for study in study_list:
            try:
                if dsn:
                    raw_id = insert_study_into_db(study, dsn=dsn)
                    print(f"Inserted study raw_id={raw_id} nct={study.get('protocolSection',{}).get('identificationModule',{}).get('nctId')}")
                else:
                    # no DB configured; write individual study to files
                    sid = study.get('protocolSection',{}).get('identificationModule',{}).get('nctId') or inserted
                    with open(f"sample_study_{sid}.json", "w", encoding="utf-8") as f:
                        f.write(json.dumps(study, indent=2))
                inserted += 1
            except Exception as e:
                print("Failed inserting study:", e)

        print(f"Processed {len(study_list)} studies, attempted inserts: {inserted}")

        next_page_token = next_token
        if not next_page_token:
            break

if __name__ == "__main__":    
    main()