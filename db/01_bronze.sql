CREATE SCHEMA IF NOT EXISTS bronze;
 
CREATE TABLE IF NOT EXISTS bronze.raw_trials (
    id          SERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    payload     JSONB NOT NULL
);