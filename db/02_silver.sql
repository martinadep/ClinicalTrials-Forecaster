
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.trials (
    nct_id                    TEXT PRIMARY KEY,
    brief_title               TEXT,
    brief_summary             TEXT,
    study_type                TEXT,
    primary_purpose           TEXT,
    overall_status            TEXT,
    lead_sponsor_class        TEXT,
    phase                     TEXT,
    enrollment_count          INTEGER,   
    start_date                DATE,              
    primary_completion_date   DATE,              
    sex                       TEXT,
    minimum_age_years         NUMERIC,
    maximum_age_years         NUMERIC,
    enrollment_duration_months NUMERIC,
    trial_velocity            NUMERIC,   
    mesh_conditions_ids       TEXT[],
    transformed_at            TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS silver.trial_sites (
    id                        SERIAL PRIMARY KEY,
    nct_id                    TEXT,
    facility_name             TEXT,
    city                      TEXT,
    state                     TEXT,
    zip                       TEXT,
    country                   TEXT,
    latitude                  NUMERIC,
    longitude                 NUMERIC,
    mesh_conditions_ids       TEXT[],
    transformed_at            TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT trial_sites_nct_id_fkey 
        FOREIGN KEY (nct_id) REFERENCES silver.trials(nct_id) 
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_silver_sites_geo ON silver.trial_sites(country, state, city, zip);
CREATE INDEX IF NOT EXISTS idx_silver_sites_nct_id ON silver.trial_sites(nct_id);