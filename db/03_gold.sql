CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_mesh_conditions (
    mesh_condition_id   TEXT PRIMARY KEY,
    mesh_condition_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.site_history (
    country           TEXT,
    city              TEXT,      
    state             TEXT,      
    zip               TEXT,     
    facility_name     TEXT, 
    latitude          NUMERIC,   
    longitude         NUMERIC,
    n_trials          INTEGER,   
    avg_velocity      NUMERIC,   
    last_year         INTEGER, 
    
    PRIMARY KEY (country, city, zip)
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
    duration_months           NUMERIC,
    mesh_conditions_ids       TEXT[],
    has_non_diagnostic_condition BOOLEAN,
    avg_site_exp              NUMERIC, 
    avg_site_vel              NUMERIC, 
    target_velocity           NUMERIC 
);

CREATE TABLE IF NOT EXISTS gold.site_conditions_history (
    country                  TEXT,
    city                     TEXT,
    zip                      TEXT,
    mesh_condition_id        TEXT, 
    n_trials_for_condition   INTEGER, 
    
    PRIMARY KEY (country, city, zip, mesh_condition_id), 
    FOREIGN KEY (country, city, zip) REFERENCES gold.site_history(country, city, zip) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_gold_sch_mesh_id ON gold.site_conditions_history(mesh_condition_id);
CREATE INDEX IF NOT EXISTS idx_gold_target ON gold.trial_features(target_velocity);
CREATE INDEX IF NOT EXISTS idx_gold_tf_mesh_array ON gold.trial_features USING gin(mesh_conditions_ids);