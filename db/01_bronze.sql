CREATE SCHEMA IF NOT EXISTS bronze;
 
CREATE TABLE IF NOT EXISTS bronze.raw_trials (
    id           SERIAL PRIMARY KEY,
    nct_id       TEXT UNIQUE,
    payload_hash TEXT,
    received_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    payload      JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.trials (
    nct_id                    TEXT PRIMARY KEY,
    brief_title               TEXT,
    brief_summary             TEXT,
    conditions                JSONB,
    mesh_conditions           JSONB,
    study_type                TEXT,
    phases                    TEXT[],
    primary_purpose           TEXT,
    enrollment_count          INTEGER,
    overall_status            TEXT,
    start_date                TEXT,
    primary_completion_date   TEXT,
    lead_sponsor_class        TEXT,
    collaborator_names        TEXT[],
    eligibility_criteria      TEXT,
    healthy_volunteers        TEXT,
    sex                       TEXT,
    minimum_age               TEXT,
    maximum_age               TEXT,
    locations                 JSONB,
    updated_at                TIMESTAMPTZ DEFAULT NOW(),
    created_at                TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trials_nct_id ON bronze.trials(nct_id);