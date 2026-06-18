CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.site_history (
    facility_name             TEXT,
    country                   TEXT,
    n_trials                  INTEGER, 
    avg_velocity              NUMERIC, 
    last_year                 INTEGER, 
    PRIMARY KEY (facility_name, country)
);

CREATE TABLE IF NOT EXISTS gold.trial_features (
    nct_id                    TEXT PRIMARY KEY,
    study_type                TEXT,
    primary_purpose           TEXT,
    lead_sponsor_class        TEXT,
    sex                       TEXT,
    healthy_volunteers        BOOLEAN,
    
    -- engineered features
    enrollment_count          INTEGER,
    num_facilities            INTEGER, 
    num_collaborators         INTEGER, 
    num_conditions            INTEGER, 
    duration_months           NUMERIC,
    avg_site_vel              NUMERIC, 
    
    -- TARGET Y --------
    TARGET_velocity           NUMERIC 
);

CREATE INDEX IF NOT EXISTS idx_gold_target ON gold.trial_features(TARGET_velocity);