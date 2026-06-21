CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.site_history (
    country                   TEXT,
    city                      TEXT,      
    state                     TEXT,      
    zip                       TEXT,     
    facility_name             TEXT, 
    latitude                  NUMERIC,   
    longitude                 NUMERIC,
    n_trials                  INTEGER,   
    avg_velocity              NUMERIC,   
    last_year                 INTEGER, 
    
    PRIMARY KEY (country, city, zip)
);

CREATE TABLE IF NOT EXISTS gold.site_conditions_history (
    country                   TEXT,
    city                      TEXT,
    zip                       TEXT,
    condition_name            TEXT,    
    n_trials_for_condition    INTEGER, 
    
    PRIMARY KEY (country, city, zip, condition_name),
    FOREIGN KEY (country, city, zip) REFERENCES gold.site_history(country, city, zip) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gold.trial_features (
    nct_id                    TEXT PRIMARY KEY,
    study_type                TEXT,
    primary_purpose           TEXT,
    lead_sponsor_class        TEXT,
    sex                       TEXT,
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