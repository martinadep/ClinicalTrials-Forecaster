CREATE SCHEMA IF NOT EXISTS bronze;
 
CREATE TABLE IF NOT EXISTS bronze.raw_trials (
    id           SERIAL PRIMARY KEY,
    nct_id       TEXT UNIQUE,
    payload_hash TEXT,
    received_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    payload      JSONB NOT NULL
);
