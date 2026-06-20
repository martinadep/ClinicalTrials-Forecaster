"""
Silver -> Gold ETL job.

Batch-reads the full silver.trials / silver.trial_sites tables from Postgres via
Spark JDBC (no Kafka involved -- gold is an aggregate over the whole dataset, so
it's inherently a full-table batch job, not a per-message one), computes the gold
tables, and writes them back to Postgres.

Run via spark-submit (see docker-compose `spark` service) from the project root so
that `shared.*` imports resolve, e.g.:

    spark-submit --master local[*] \
        --packages org.postgresql:postgresql:42.7.3 \
        spark_jobs/silver_to_gold.py

Only the Postgres JDBC driver is needed (no spark-sql-kafka package), since this
job neither reads nor writes Kafka.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import load_dotenv
from shared.db import build_dsn_from_env, build_jdbc_url_from_env


def read_table(spark, jdbc_url, properties, dbtable):
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)


def truncate_tables(table_names):
    """Truncate gold tables before a full recompute.

    Postgres requires a table and anything FK-referencing it to be truncated in the
    same statement, so all tables are passed to a single TRUNCATE call.
    """
    import psycopg2

    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {', '.join(table_names)}")
    finally:
        conn.close()


def upsert_trial_features_partition(rows):
    """Upsert a partition of gold.trial_features rows, keyed by nct_id."""
    import psycopg2
    import psycopg2.extras

    rows = list(rows)
    if not rows:
        return
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO gold.trial_features (
                        nct_id, study_type, primary_purpose, lead_sponsor_class, sex,
                        healthy_volunteers, phase, enrollment_count, n_sites, num_conditions,
                        duration_months, avg_site_exp, avg_site_vel, target_velocity
                    ) VALUES %s
                    ON CONFLICT (nct_id) DO UPDATE SET
                        study_type = EXCLUDED.study_type,
                        primary_purpose = EXCLUDED.primary_purpose,
                        lead_sponsor_class = EXCLUDED.lead_sponsor_class,
                        sex = EXCLUDED.sex,
                        healthy_volunteers = EXCLUDED.healthy_volunteers,
                        phase = EXCLUDED.phase,
                        enrollment_count = EXCLUDED.enrollment_count,
                        n_sites = EXCLUDED.n_sites,
                        num_conditions = EXCLUDED.num_conditions,
                        duration_months = EXCLUDED.duration_months,
                        avg_site_exp = EXCLUDED.avg_site_exp,
                        avg_site_vel = EXCLUDED.avg_site_vel,
                        target_velocity = EXCLUDED.target_velocity
                    """,
                    [
                        (
                            r.nct_id, r.study_type, r.primary_purpose, r.lead_sponsor_class, r.sex,
                            r.healthy_volunteers, r.phase, r.enrollment_count, r.n_sites, r.num_conditions,
                            r.duration_months, r.avg_site_exp, r.avg_site_vel, r.target_velocity,
                        )
                        for r in rows
                    ],
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                )
    finally:
        conn.close()


def main():
    load_dotenv()
    jdbc_url, jdbc_props = build_jdbc_url_from_env()
    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

    trials_df = read_table(spark, jdbc_url, jdbc_props, "silver.trials").cache()
    sites_df = read_table(spark, jdbc_url, jdbc_props, "silver.trial_sites").cache()
    # Pushed down to Postgres: phases[1] avoids dealing with Postgres TEXT[] <-> Spark
    # array-type JDBC mapping for a single scalar value; jsonb_array_length() is the
    # fallback source for num_conditions on trials with zero silver.trial_sites rows.
    bronze_df = read_table(
        spark, jdbc_url, jdbc_props,
        """
        (SELECT nct_id, phases[1] AS phase,
                jsonb_array_length(coalesce(conditions, '[]'::jsonb)) AS bronze_num_conditions
         FROM bronze.trials) AS bronze_trials
        """,
    )
    # conditions is denormalized onto every site row by bronze_to_silver.py, so any one
    # site's array length equals the trial's condition count -- read it pre-flattened
    # here rather than pulling the raw TEXT[] into Spark.
    site_conditions_count_df = read_table(
        spark, jdbc_url, jdbc_props,
        "(SELECT nct_id, array_length(conditions, 1) AS silver_num_conditions FROM silver.trial_sites) AS sc",
    )
    # Pre-flattened (nct_id, condition) pairs for gold.site_conditions_history, again
    # avoiding Spark-side handling of the Postgres TEXT[] column.
    exploded_conditions_df = read_table(
        spark, jdbc_url, jdbc_props,
        """
        (SELECT nct_id, facility_name, city, state, zip, country, unnest(conditions) AS condition_name
         FROM silver.trial_sites
         WHERE conditions IS NOT NULL AND country IS NOT NULL AND city IS NOT NULL AND zip IS NOT NULL
        ) AS sc_exploded
        """,
    )

    total_trials = trials_df.count()
    total_sites = sites_df.count()

    sites_with_geo_key = sites_df.filter(
        F.col("country").isNotNull() & F.col("city").isNotNull() & F.col("zip").isNotNull()
    )
    skipped_sites_no_geo_key = total_sites - sites_with_geo_key.count()

    # ---- Step 1: per-trial velocity is already computed by bronze_to_silver.py ----
    # silver.trials.trial_velocity IS the target velocity; enrollment_duration_months
    # IS the gold duration_months. No recomputation needed here.
    null_velocity_count = trials_df.filter(F.col("trial_velocity").isNull()).count()

    # ---- Step 2a: gold.site_history ----
    # Back-assign each trial's velocity onto its sites, then aggregate per (country, city, zip).
    sites_with_velocity = sites_with_geo_key.join(
        trials_df.select("nct_id", "trial_velocity", "start_date"), on="nct_id", how="left"
    )
    site_history_df = (
        sites_with_velocity.groupBy("country", "city", "zip")
        .agg(
            F.first("facility_name", ignorenulls=True).alias("facility_name"),
            F.first("state", ignorenulls=True).alias("state"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.first("longitude", ignorenulls=True).alias("longitude"),
            F.countDistinct("nct_id").alias("n_trials"),
            F.avg("trial_velocity").alias("avg_velocity"),
            F.max(F.year("start_date")).alias("last_year"),
        )
        .cache()
    )

    # ---- Step 2b: gold.site_conditions_history ----
    site_conditions_history_df = (
        exploded_conditions_df.groupBy("country", "city", "zip", "condition_name")
        .agg(F.countDistinct("nct_id").alias("n_trials_for_condition"))
    )

    truncate_tables(["gold.site_conditions_history", "gold.site_history"])
    site_history_df.write.jdbc(url=jdbc_url, table="gold.site_history", mode="append", properties=jdbc_props)
    site_conditions_history_df.write.jdbc(
        url=jdbc_url, table="gold.site_conditions_history", mode="append", properties=jdbc_props
    )
    site_history_rows_written = site_history_df.count()

    # ---- Step 3: gold.trial_features (uses Step 2's site_history) ----
    num_conditions_df = (
        site_conditions_count_df.groupBy("nct_id")
        .agg(F.first("silver_num_conditions", ignorenulls=True).alias("silver_num_conditions"))
    )

    n_sites_df = sites_df.groupBy("nct_id").agg(F.count("*").alias("n_sites"))

    # avg_site_exp / avg_site_vel: average the just-written site_history stats across
    # each trial's own sites.
    # KNOWN LIMITATION (accepted project simplification, not fixed here): site_history is
    # computed over ALL-TIME trials, including the trial's own velocity/count. This is
    # mild target leakage for avg_site_vel in particular. A production version would
    # compute site stats using only trials completed before this trial's start_date.
    site_stats_per_trial = (
        sites_with_geo_key.join(
            site_history_df.select("country", "city", "zip", "n_trials", "avg_velocity"),
            on=["country", "city", "zip"],
            how="left",
        )
        .groupBy("nct_id")
        .agg(
            F.avg("n_trials").alias("avg_site_exp"),
            F.avg("avg_velocity").alias("avg_site_vel"),
        )
    )

    trial_features_df = (
        trials_df.select(
            "nct_id", "study_type", "primary_purpose", "lead_sponsor_class", "sex",
            "healthy_volunteers", "enrollment_count", "enrollment_duration_months", "trial_velocity",
        )
        .withColumnRenamed("enrollment_duration_months", "duration_months")
        .withColumnRenamed("trial_velocity", "target_velocity")
        .join(bronze_df, on="nct_id", how="left")
        .join(num_conditions_df, on="nct_id", how="left")
        .join(n_sites_df, on="nct_id", how="left")
        .join(site_stats_per_trial, on="nct_id", how="left")
        .withColumn("n_sites", F.coalesce(F.col("n_sites"), F.lit(0)))
        .withColumn("num_conditions", F.coalesce(F.col("silver_num_conditions"), F.col("bronze_num_conditions")))
        .select(
            "nct_id", "study_type", "primary_purpose", "lead_sponsor_class", "sex",
            "healthy_volunteers", "phase", "enrollment_count", "n_sites", "num_conditions",
            "duration_months", "avg_site_exp", "avg_site_vel", "target_velocity",
        )
    )

    trial_features_df.foreachPartition(upsert_trial_features_partition)
    trial_features_rows_written = trial_features_df.count()

    print(
        f"[INFO]: silver_to_gold: trials_read={total_trials} sites_read={total_sites} "
        f"sites_skipped(no country/city/zip)={skipped_sites_no_geo_key} "
        f"trials_with_null_velocity={null_velocity_count} "
        f"site_history_written={site_history_rows_written} "
        f"trial_features_written={trial_features_rows_written}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
