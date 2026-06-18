
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.trials (
    nct_id                    TEXT PRIMARY KEY,
    brief_title               TEXT,
    brief_summary             TEXT,
    study_type                TEXT,
    primary_purpose           TEXT,
    overall_status            TEXT,
    lead_sponsor_class        TEXT,
    enrollment_count          INTEGER,   
    start_date                DATE,              
    primary_completion_date   DATE,            
    healthy_volunteers        BOOLEAN,           
    sex                       TEXT,
    minimum_age_years         NUMERIC,
    maximum_age_years         NUMERIC,
    enrollment_duration_days  INTEGER,   
    transformed_at            TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS silver.trial_sites (
    id                        SERIAL PRIMARY KEY,
    nct_id                    TEXT REFERENCES silver.trials(nct_id) ON DELETE CASCADE,
    facility_name             TEXT,
    city                      TEXT,
    state                     TEXT,
    country                   TEXT,
    transformed_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_duration ON silver.trials(enrollment_duration_days);
CREATE INDEX IF NOT EXISTS idx_sites_geo ON silver.trial_sites(country, state, city);
CREATE INDEX IF NOT EXISTS idx_sites_facility ON silver.trial_sites(facility_name);