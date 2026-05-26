CREATE SCHEMA IF NOT EXISTS bronze;
 
CREATE TABLE IF NOT EXISTS bronze.raw_trials (
    id          SERIAL PRIMARY KEY,
    nct_id      TEXT UNIQUE,
    payload_hash TEXT,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    payload     JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.trials (
    id                        SERIAL PRIMARY KEY,
    raw_id                    INTEGER REFERENCES bronze.raw_trials(id) ON DELETE SET NULL,
    nct_id                    TEXT UNIQUE,
    payload_hash              TEXT,
    brief_title               TEXT,
    official_title            TEXT,
    acronym                   TEXT,
    conditions                JSONB,
    keywords                  JSONB,
    study_type                TEXT,
    phases                    TEXT[],
    allocation                TEXT,
    intervention_model        TEXT,
    primary_purpose           TEXT,
    enrollment_count          INTEGER,
    enrollment_type           TEXT,
    overall_status            TEXT,
    start_date                DATE,
    primary_completion_date   DATE,
    completion_date           DATE,
    study_first_post_date     DATE,
    last_update_post_date     DATE,
    lead_sponsor              JSONB,
    organization_class        TEXT,
    responsible_party         JSONB,
    eligibility_criteria      TEXT,
    healthy_volunteers        BOOLEAN,
    sex                       TEXT,
    minimum_age               TEXT,
    maximum_age               TEXT,
    locations                 JSONB,
    version_holder            TEXT,
    updated_at                TIMESTAMPTZ DEFAULT NOW(),
    created_at                TIMESTAMPTZ DEFAULT NOW()
);