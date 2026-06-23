import os
import psycopg2
import psycopg2.extras
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

def build_jdbc_url_from_env():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    port = os.getenv("POSTGRES_PORT", "5432")
    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or "localhost"
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}
    return jdbc_url, properties

def generic_upsert_partition(rows, query, fields, template=None):
    """
    Funzione core universale per eseguire upsert massivi dai nodi worker di Spark.
    Evita la ripetizione di codice boilerplate per ogni tabella.
    """
    rows = list(rows)
    if not rows: 
        return
    
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            data_batch = [
                tuple(getattr(r, field, None) for field in fields)
                for r in rows
            ]
            psycopg2.extras.execute_values(cur, query, data_batch, template=template)
    finally: 
        conn.close()

def insert_bronze_studies_bulk(records, dsn=None):
    if not records:
        return

    dsn = dsn or build_dsn_from_env()
    if not dsn:
        raise ValueError("No DSN provided and DATABASE_URL/env variables not set")

    raw_trials_batch = []
    trials_batch = []

    for study in records:
        try:
            fields = extract_trial_fields(study)
            nct_id = fields.get("nct_id")
            if not nct_id:
                continue

            raw_payload = psycopg2.extras.Json(study)
            payload_hash = fields.get("payload_hash")
            raw_trials_batch.append((nct_id, payload_hash, raw_payload))

            lead_sponsor_class = fields.get("lead_sponsor_class") or fields.get("organization_class")
            
            trials_batch.append((
                fields["nct_id"], fields["brief_title"], fields.get("brief_summary"),
                fields["conditions"], fields["mesh_conditions"], fields["study_type"],
                fields["phases"], fields["primary_purpose"], fields["enrollment_count"],
                fields["overall_status"], fields["start_date"], fields["primary_completion_date"],
                lead_sponsor_class, fields.get("collaborator_names"), fields["eligibility_criteria"],
                fields["healthy_volunteers"], fields["sex"], fields["minimum_age"],
                fields["maximum_age"], fields["locations"]
            ))
        except Exception as e:
            print(f"[WARN PARSING]: Impossibile mappare il record per il bulk: {e}")
            continue

    if not raw_trials_batch:
        return

    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO bronze.raw_trials (nct_id, payload_hash, payload)
                VALUES %s ON CONFLICT (nct_id) DO UPDATE SET
                    payload = EXCLUDED.payload, payload_hash = EXCLUDED.payload_hash, updated_at = NOW()
                """,
                raw_trials_batch, template="(%s, %s, %s)"
            )

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO bronze.trials (
                    nct_id, brief_title, brief_summary, conditions, mesh_conditions, study_type, phases,
                    primary_purpose, enrollment_count, overall_status, start_date, primary_completion_date, 
                    lead_sponsor_class, collaborator_names, eligibility_criteria, healthy_volunteers, sex, 
                    minimum_age, maximum_age, locations
                ) VALUES %s ON CONFLICT (nct_id) DO UPDATE SET
                    brief_title = EXCLUDED.brief_title, brief_summary = EXCLUDED.brief_summary, conditions = EXCLUDED.conditions,
                    mesh_conditions = EXCLUDED.mesh_conditions, study_type = EXCLUDED.study_type, phases = EXCLUDED.phases,
                    primary_purpose = EXCLUDED.primary_purpose, enrollment_count = EXCLUDED.enrollment_count, overall_status = EXCLUDED.overall_status,
                    start_date = EXCLUDED.start_date, primary_completion_date = EXCLUDED.primary_completion_date, lead_sponsor_class = EXCLUDED.lead_sponsor_class,
                    collaborator_names = EXCLUDED.collaborator_names, eligibility_criteria = EXCLUDED.eligibility_criteria, healthy_volunteers = EXCLUDED.healthy_volunteers,
                    sex = EXCLUDED.sex, minimum_age = EXCLUDED.minimum_age, maximum_age = EXCLUDED.maximum_age, locations = EXCLUDED.locations, updated_at = NOW()
                """,
                trials_batch, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
    finally:
        conn.close()

def upsert_trials_partition(rows):
    query = """
    INSERT INTO silver.trials (
        nct_id, brief_title, brief_summary, study_type, primary_purpose, overall_status, 
        lead_sponsor_class, enrollment_count, start_date, primary_completion_date, sex, 
        minimum_age_years, maximum_age_years, enrollment_duration_months, trial_velocity, 
        phase, mesh_conditions_ids, transformed_at
    ) VALUES %s ON CONFLICT (nct_id) DO UPDATE SET
        brief_title = EXCLUDED.brief_title, brief_summary = EXCLUDED.brief_summary, study_type = EXCLUDED.study_type, 
        primary_purpose = EXCLUDED.primary_purpose, overall_status = EXCLUDED.overall_status, lead_sponsor_class = EXCLUDED.lead_sponsor_class,
        enrollment_count = EXCLUDED.enrollment_count, start_date = EXCLUDED.start_date, primary_completion_date = EXCLUDED.primary_completion_date, 
        sex = EXCLUDED.sex, minimum_age_years = EXCLUDED.minimum_age_years, maximum_age_years = EXCLUDED.maximum_age_years,
        enrollment_duration_months = EXCLUDED.enrollment_duration_months, trial_velocity = EXCLUDED.trial_velocity,
        phase = EXCLUDED.phase, mesh_conditions_ids = EXCLUDED.mesh_conditions_ids, transformed_at = EXCLUDED.transformed_at
    """
    fields = [
        "nct_id", "brief_title", "brief_summary", "study_type", "primary_purpose", "overall_status",
        "lead_sponsor_class", "enrollment_count", "start_date", "primary_completion_date", "sex",
        "minimum_age_years", "maximum_age_years", "enrollment_duration_months", "trial_velocity",
        "phase", "mesh_conditions_ids" 
    ]
    generic_upsert_partition(rows, query, fields, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())")

def delete_and_insert_sites_partition(rows):
    rows = list(rows)
    if not rows: return
    nct_ids = list(set(r.nct_id for r in rows if r.nct_id))
    if not nct_ids: return
    
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM silver.trial_sites WHERE nct_id = ANY(%s)", (nct_ids,))
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO silver.trial_sites (nct_id, facility_name, city, state, zip, country, latitude, longitude, mesh_conditions_ids, transformed_at)
                VALUES %s
                """,
                [(r.nct_id, r.facility_name, r.city, r.state, r.zip, r.country, r.latitude, r.longitude, r.mesh_conditions_ids) for r in rows],
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
            )
    finally: conn.close()

def upsert_mesh_dimension_partition(rows):
    query = "INSERT INTO gold.dim_mesh_conditions (mesh_condition_id, mesh_condition_name) VALUES %s ON CONFLICT (mesh_condition_id) DO NOTHING"
    fields = ["mesh_condition_id", "mesh_condition_name"]
    generic_upsert_partition(rows, query, fields, template="(%s, %s)")

def upsert_trial_features_partition(rows):
    query = """
    INSERT INTO gold.trial_features (
        nct_id, study_type, primary_purpose, lead_sponsor_class, sex, phase, enrollment_count, 
        n_sites, duration_months, mesh_conditions_ids, avg_site_exp, avg_site_vel, target_velocity
    ) VALUES %s ON CONFLICT (nct_id) DO UPDATE SET 
        study_type = EXCLUDED.study_type, primary_purpose = EXCLUDED.primary_purpose, lead_sponsor_class = EXCLUDED.lead_sponsor_class, 
        sex = EXCLUDED.sex, phase = EXCLUDED.phase, enrollment_count = EXCLUDED.enrollment_count, n_sites = EXCLUDED.n_sites, 
        duration_months = EXCLUDED.duration_months, mesh_conditions_ids = EXCLUDED.mesh_conditions_ids, avg_site_exp = EXCLUDED.avg_site_exp, 
        avg_site_vel = EXCLUDED.avg_site_vel, target_velocity = EXCLUDED.target_velocity
    """
    fields = [
        "nct_id", "study_type", "primary_purpose", "lead_sponsor_class", "sex", "phase",
        "enrollment_count", "n_sites", "duration_months", "mesh_conditions_ids", "avg_site_exp",
        "avg_site_vel", "target_velocity"
    ]
    generic_upsert_partition(rows, query, fields, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

def truncate_tables(table_names):
    if not table_names: return
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            tables_str = ", ".join(table_names)
            cur.execute(f"TRUNCATE TABLE {tables_str} CASCADE")
    finally: conn.close()