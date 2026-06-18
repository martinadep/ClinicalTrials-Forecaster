import os
from ingestion.transformer import extract_trial_fields

def build_dsn_from_env():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    port = os.getenv("POSTGRES_PORT", "5432")
    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or "localhost"
    if user and password and db:
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return None


def _upsert_raw_trial(cur, fields, raw_payload):
    """Insert or update bronze.raw_trials row and return raw_id."""
    nct_id = fields["nct_id"]
    payload_hash = fields["payload_hash"]

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
        print(f"[INFO]: Updated raw trial with id {raw_id} for nct_id {nct_id}")
        return raw_id

    cur.execute(
        """
        INSERT INTO bronze.raw_trials (nct_id, payload_hash, payload)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (nct_id, payload_hash, raw_payload),
    )
    raw_id = cur.fetchone()[0]
    print(f"[INFO]: Inserted raw trial with id {raw_id} for nct_id {nct_id}")
    return raw_id


def _build_trial_values(fields, psycopg2_extras):
    """Build positional values tuple for bronze.trials write operations."""
    lead_sponsor = fields.get("lead_sponsor")
    lead_sponsor_class = None
    if isinstance(lead_sponsor, dict):
        lead_sponsor_class = lead_sponsor.get("class")
    if lead_sponsor_class is None:
        lead_sponsor_class = fields.get("organization_class")

    return (
        fields["nct_id"],
        fields["brief_title"],
        fields.get("brief_summary"),
        psycopg2_extras.Json(fields["conditions"]) if fields["conditions"] is not None else None,
        fields["study_type"],
        fields["phases"],
        fields["primary_purpose"],
        fields["enrollment_count"],
        fields["overall_status"],
        fields["start_date"],
        fields["primary_completion_date"],
        lead_sponsor_class,
        fields.get("collaborator_names"),
        fields["eligibility_criteria"],
        fields["healthy_volunteers"],
        fields["sex"],
        fields["minimum_age"],
        fields["maximum_age"],
        psycopg2_extras.Json(fields["locations"]) if fields["locations"] else None,
    )


def _upsert_trial_row(cur, trial_values, nct_id):
    """Insert or update bronze.trials row based on nct_id."""
    existing_trial = None
    if nct_id:
        cur.execute("SELECT id FROM bronze.trials WHERE nct_id = %s LIMIT 1", (nct_id,))
        existing_trial = cur.fetchone()

    if existing_trial:
        existing_id = existing_trial[0]
        cur.execute(
            """
            UPDATE bronze.trials
            SET
                nct_id = %s,
                brief_title = %s,
                brief_summary = %s,
                conditions = %s,
                study_type = %s,
                phases = %s,
                primary_purpose = %s,
                enrollment_count = %s,
                overall_status = %s,
                start_date = %s,
                primary_completion_date = %s,
                lead_sponsor_class = %s,
                collaborator_names = %s,
                eligibility_criteria = %s,
                healthy_volunteers = %s,
                sex = %s,
                minimum_age = %s,
                maximum_age = %s,
                locations = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            trial_values + (existing_id,),
        )
        return

    cur.execute(
        """
        INSERT INTO bronze.trials (
            nct_id, brief_title, brief_summary, conditions, study_type, phases,
            primary_purpose, enrollment_count, overall_status, start_date,
            primary_completion_date, lead_sponsor_class, collaborator_names,
            eligibility_criteria, healthy_volunteers, sex, minimum_age, maximum_age,
            locations
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        trial_values,
    )


def insert_study_into_db(study, dsn=None):
    """Insert or update a study in bronze raw/current tables."""
    try:
        import psycopg2
        import psycopg2.extras
    except Exception as e:
        raise RuntimeError("psycopg2 is required to insert into the database: %s" % e)

    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("No DSN provided and DATABASE_URL not set")

    fields = extract_trial_fields(study)
    raw_payload = psycopg2.extras.Json(study)

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                raw_id = _upsert_raw_trial(cur, fields, raw_payload)
                trial_values = _build_trial_values(fields, psycopg2.extras)
                _upsert_trial_row(cur, trial_values, fields["nct_id"])
                return raw_id
    finally:
        conn.close()