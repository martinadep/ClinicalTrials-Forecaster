CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.site_history (
    facility_name             TEXT,
    country                   TEXT,
    city                      TEXT,      
    state                     TEXT,      
    zip                       TEXT,      
    latitude                  NUMERIC,   
    longitude                 NUMERIC,
    n_trials                  INTEGER, 
    avg_velocity              NUMERIC, 
    last_year                 INTEGER, 
    
    PRIMARY KEY (facility_name, country, city, zip)
);

CREATE TABLE IF NOT EXISTS gold.trial_features (
    nct_id                    TEXT PRIMARY KEY,
    study_type                TEXT,
    primary_purpose           TEXT,
    lead_sponsor_class        TEXT,
    sex                       TEXT,
    healthy_volunteers        BOOLEAN,
    phase                     TEXT,
    enrollment_count          INTEGER,
    n_sites                   INTEGER, 
     
    num_conditions            INTEGER, 
    duration_months           NUMERIC,
    
    avg_site_exp              NUMERIC, -- average n_trials of participating sites
    avg_site_vel              NUMERIC, -- average velocity of participating sites
    
    -- TARGET Y --------
    TARGET_velocity           NUMERIC 
);

CREATE INDEX IF NOT EXISTS idx_gold_target ON gold.trial_features(TARGET_velocity);