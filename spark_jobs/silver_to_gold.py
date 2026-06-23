from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import load_dotenv
from shared.db import build_dsn_from_env, build_jdbc_url_from_env


def read_table(spark, jdbc_url, properties, dbtable):
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)


def truncate_tables(table_names):
    """Truncate gold tables before a full recompute."""
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
                        phase, enrollment_count, n_sites, duration_months,
                        mesh_conditions_ids, has_non_diagnostic_condition,
                        avg_site_exp, avg_site_vel, target_velocity
                    ) VALUES %s
                    ON CONFLICT (nct_id) DO UPDATE SET
                        study_type = EXCLUDED.study_type,
                        primary_purpose = EXCLUDED.primary_purpose,
                        lead_sponsor_class = EXCLUDED.lead_sponsor_class,
                        sex = EXCLUDED.sex,
                        phase = EXCLUDED.phase,
                        enrollment_count = EXCLUDED.enrollment_count,
                        n_sites = EXCLUDED.n_sites,
                        duration_months = EXCLUDED.duration_months,
                        mesh_conditions_ids = EXCLUDED.mesh_conditions_ids,
                        has_non_diagnostic_condition = EXCLUDED.has_non_diagnostic_condition,
                        avg_site_exp = EXCLUDED.avg_site_exp,
                        avg_site_vel = EXCLUDED.avg_site_vel,
                        target_velocity = EXCLUDED.target_velocity
                    """,
                    [
                        (
                            r.nct_id, r.study_type, r.primary_purpose, r.lead_sponsor_class, r.sex,
                            r.phase, r.enrollment_count, r.n_sites, r.duration_months,
                            r.mesh_conditions_ids, r.has_non_diagnostic_condition,
                            r.avg_site_exp, r.avg_site_vel, r.target_velocity,
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
    
    spark.sparkContext.setLogLevel("ERROR") 

    trials_df = read_table(spark, jdbc_url, jdbc_props, "silver.trials").cache()
    
    raw_sites_df = read_table(spark, jdbc_url, jdbc_props, "silver.trial_sites")
    sites_df = raw_sites_df.filter(
        F.col("facility_name").isNotNull() & 
        (F.trim(F.col("facility_name")) != "") &
        (F.upper(F.trim(F.col("facility_name"))) != "UNKNOWN FACILITY")
    ).cache()

    trials_df = trials_df.filter((F.col("trial_velocity") >= 0) & (F.col("trial_velocity") < 150))
    
    exploded_conditions_df = read_table(
        spark, jdbc_url, jdbc_props,
        """
        (SELECT nct_id, facility_name, city, state, zip, country, unnest(mesh_conditions_ids) AS condition_id
         FROM silver.trial_sites
         WHERE mesh_conditions_ids IS NOT NULL AND country IS NOT NULL AND city IS NOT NULL AND zip IS NOT NULL
           AND facility_name IS NOT NULL 
           AND TRIM(facility_name) != ''
           AND UPPER(TRIM(facility_name)) != 'UNKNOWN FACILITY'
        ) AS sc_exploded
        """,
    )
    
    total_trials = trials_df.count()
    total_sites = sites_df.count()

    sites_with_geo_key = sites_df.filter(
        F.col("country").isNotNull() & F.col("city").isNotNull() & F.col("zip").isNotNull()
    )
    skipped_sites_no_geo_key = total_sites - sites_with_geo_key.count()

    null_velocity_count = trials_df.filter(F.col("trial_velocity").isNull()).count()

    sites_with_velocity = sites_with_geo_key.join(
        trials_df.select("nct_id", "trial_velocity", "start_date"), on="nct_id", how="left"
    )
    
    sites_with_velocity = sites_with_velocity.withColumn(
        "trial_velocity", F.coalesce(F.col("trial_velocity"), F.lit(0.0))
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

    site_conditions_history_df = (
        exploded_conditions_df.groupBy("country", "city", "zip", "condition_id")
        .agg(F.countDistinct("nct_id").alias("n_trials_for_condition"))
        .withColumnRenamed("condition_id", "mesh_condition_id")
    )

    truncate_tables(["gold.site_conditions_history", "gold.site_history"])
    site_history_df.write.jdbc(url=jdbc_url, table="gold.site_history", mode="append", properties=jdbc_props)
    site_conditions_history_df.select(
        "country", "city", "zip", "mesh_condition_id", "n_trials_for_condition"
    ).write.jdbc(url=jdbc_url, table="gold.site_conditions_history", mode="append", properties=jdbc_props)
    
    site_history_rows_written = site_history_df.count()
    
    n_sites_df = sites_df.groupBy("nct_id").agg(F.count("*").alias("n_sites"))

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
    
    raw_features_df = (
        trials_df.select(
            "nct_id", "study_type", "primary_purpose", "lead_sponsor_class", "sex", "phase",
            "enrollment_count", "enrollment_duration_months", "trial_velocity",
            "mesh_conditions_ids", "has_non_diagnostic_condition"
        )
        .withColumnRenamed("enrollment_duration_months", "duration_months")
        .withColumnRenamed("trial_velocity", "target_velocity")
        .join(n_sites_df, on="nct_id", how="left")
        .join(site_stats_per_trial, on="nct_id", how="left")
        .withColumn("n_sites", F.coalesce(F.col("n_sites"), F.lit(0)))
    )

    total_features_before_filter = raw_features_df.count()

    trial_features_df = raw_features_df.filter(
        (F.col("n_sites") > 0) & 
        (F.col("avg_site_exp").isNotNull()) & 
        (F.col("avg_site_vel").isNotNull())
    ).select(
        "nct_id", "study_type", "primary_purpose", "lead_sponsor_class", "sex",
        "phase", "enrollment_count", "n_sites", "duration_months",
        "mesh_conditions_ids", "has_non_diagnostic_condition",
        "avg_site_exp", "avg_site_vel", "target_velocity"
    )

    trial_features_rows_written = trial_features_df.count()
    removed_trials_count = total_features_before_filter - trial_features_rows_written

    trial_features_df.foreachPartition(upsert_trial_features_partition)

    print(
        f"[INFO]: silver_to_gold: trials_read={total_trials} sites_read={total_sites} \n"
        f"[INFO]: sites_skipped(no country/city/zip)={skipped_sites_no_geo_key} \n"
        f"[INFO]: trials_with_null_velocity={null_velocity_count} \n"
        f"[INFO]: site_history_written={site_history_rows_written} \n"
        f"[INFO]: -------------------------------------------------------- \n"
        f"[INFO]: FILTER REPORT FOR ML (gold.trial_features): \n"
        f"[INFO]:   -> Total available trials: {total_features_before_filter} \n"
        f"[INFO]:   -> DISCARDED: {removed_trials_count} \n"
        f"[INFO]:   -> KEPT: {trial_features_rows_written} "
        f"({((trial_features_rows_written/total_features_before_filter)*100):.2f}% del totale)"
    )

    spark.stop()


if __name__ == "__main__":
    main()