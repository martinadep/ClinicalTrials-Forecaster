# spark_jobs

Spark ETL jobs for the medallion pipeline: `bronze_to_silver.py` and `silver_to_gold.py`.
Both run as one-shot batch jobs via `spark-submit` inside the `spark` Docker Compose
service (local mode — no separate master/worker cluster, see root `docker-compose.yml`).

## Prerequisites

```bash
docker compose up -d
```

`bronze_to_silver.py` needs `bronze.trials` and the `trials.bronze` Kafka topic
populated first:

```bash
python -m ingestion.fetcher
```

## `bronze_to_silver.py`

**What it does:** batch-reads study JSON from Kafka topic `trials.bronze`
(`earliest`→`latest`, not streaming — ingestion is daily, so there's no need to keep
a long-running consumer), parses each message, normalizes fields, and writes:

* `silver.trials` — one row per trial, upserted by `nct_id`
* `silver.trial_sites` — one row per (trial, location), existing rows for a trial's
  `nct_id` deleted then reinserted on each run (avoids stale/duplicate sites)
* re-publishes the cleaned trial to Kafka topic `trials.silver`, keyed by `nct_id`

Dedup is by Kafka message **timestamp** (not offset, since offsets aren't globally
ordered across partitions) — if the same `nct_id` appears twice in one run, the
message with the latest timestamp wins.

Field-level logic worth knowing:
* `minimum_age_years`/`maximum_age_years` — parsed from strings like `"18 Years"`/
  `"6 Months"` via `shared/transforms.py::parse_age_to_years`
* `start_date`/`primary_completion_date` — normalized to `YYYY-MM-DD` via
  `shared/transforms.py::normalize_date` (handles year-only/year-month source dates)
* `enrollment_duration_months` — months between start and primary completion date
  (days ÷ 30.44), `NULL` if either date is missing or the range is negative
* `trial_velocity` — `enrollment_count / enrollment_duration_months` (patients/month),
  `NULL` if either input is missing or zero
* `silver.trial_sites.conditions` — the trial's condition list, denormalized onto
  every site row (used later by `silver_to_gold.py` for `gold.site_conditions_history`)

**Run it:**

```bash
docker exec --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver.py"
```

Look for this line near the end of the output (everything else is normal Spark/Kafka
log noise):

```
[INFO]: bronze_to_silver: processed=<n> written=<n> skipped(no nct_id/unparseable)=<n>
```

## `silver_to_gold.py`

**What it does:** unlike the bronze job, this one reads from **Postgres, not Kafka**
— `gold.site_history` is an aggregate over the *entire* dataset, which needs the
whole table in hand, so it's inherently a full batch read/write, not a per-message
job. It reads `silver.trials`/`silver.trial_sites` (plus a small `bronze.trials`
lookup for `phases`/`conditions`, pushed down as SQL rather than read into Spark) via
JDBC, and writes:

* `gold.site_history` — one row per `(country, city, zip)`, with `n_trials`,
  `avg_velocity` (mean trial velocity across that site's trials), `last_year`
* `gold.site_conditions_history` — one row per `(country, city, zip, condition_name)`,
  with `n_trials_for_condition`
* `gold.trial_features` — one row per trial, the model input table

`site_history`/`site_conditions_history` are **truncated and fully reinserted** on
every run (not upserted) — since both are recomputed from the complete silver
tables each time, an incremental upsert would just leave stale rows behind for any
facility/condition no longer present in silver. Postgres requires the two tables to
be truncated in a single statement (one FKs the other), so they're always truncated
together. `gold.trial_features` is upserted by `nct_id` instead, since there's no FK
ordering constraint forcing a full-table rewrite there.

Computation order matters: `trial_features.avg_site_exp`/`avg_site_vel` are computed
by joining each trial's sites back to `site_history`, so `site_history` is always
written first within the same run.

**Known limitation (accepted simplification, not fixed):** `avg_site_vel` is computed
from all-time `site_history`, which includes the trial's own velocity — mild target
leakage. See the comment at that computation in the script; a production version
would compute site stats using only trials completed before each trial's `start_date`.

**Data-gap fallbacks** (flagged because they reach outside `silver`, not a silent
design choice):
* `phase` has no column in `silver.trials` at all — pulled from
  `bronze.trials.phases[1]` (first phase, since gold wants a single value, not an
  array)
* `num_conditions` normally comes from `silver.trial_sites.conditions` (same value on
  every site row for a trial), but falls back to
  `jsonb_array_length(bronze.trials.conditions)` for trials with zero rows in
  `silver.trial_sites`

**Run it:**

```bash
docker exec --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.3 spark_jobs/silver_to_gold.py"
```

Note: no `spark-sql-kafka` package needed here — this job never touches Kafka.

Look for this line near the end of the output:

```
[INFO]: silver_to_gold: trials_read=<n> sites_read=<n> sites_skipped(no country/city/zip)=<n> trials_with_null_velocity=<n> site_history_written=<n> trial_features_written=<n>
```

`sites_skipped` counts `silver.trial_sites` rows missing `country`/`city`/`zip` —
those are `NOT NULL` primary-key columns on `gold.site_history`, so such rows can't
be grouped into it and are excluded rather than guessed at.

## Verifying results

```bash
docker exec -i clinical_trial_db psql -U admin -d clinical_trials -c "
SELECT count(*) FROM silver.trials; SELECT count(*) FROM silver.trial_sites;
SELECT count(*) FROM gold.site_history; SELECT count(*) FROM gold.site_conditions_history;
SELECT count(*) FROM gold.trial_features;
"
```

Or browse the tables in Adminer (`http://localhost:8080`) — use the schema selector
near the top of the page to switch from `public` to `silver`/`gold`/`bronze`.

## Shared helpers used

* `shared/config.py::load_dotenv` — loads `.env`
* `shared/db.py::build_dsn_from_env` — psycopg2 DSN, used for all upsert/truncate writes
* `shared/db.py::build_jdbc_url_from_env` — Spark JDBC URL/properties, used for
  `silver_to_gold.py`'s table reads and `site_history`/`site_conditions_history` writes
* `shared/kafka.py::build_kafka_producer` — used by `bronze_to_silver.py` to
  re-publish to `trials.silver`
* `shared/transforms.py::parse_age_to_years`, `normalize_date` — reusable field
  parsers, used by `bronze_to_silver.py`
