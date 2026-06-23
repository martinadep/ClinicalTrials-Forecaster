import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import load_dotenv
from shared.db import build_jdbc_url_from_env, truncate_tables

def read_table(spark, jdbc_url, properties, dbtable):
    """Loads a database table via JDBC."""
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)

def main():
    load_dotenv()
    jdbc_url, jdbc_props = build_jdbc_url_from_env()
    TOPIC_GOLD_FEATURES = os.getenv("KAFKA_TOPIC_GOLD_FEATURES", "trials.gold")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    # Spark Session Initialization with Performance Optimization Parameters
    spark = SparkSession.builder \
        .appName("silver_to_gold") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") 

    # Read trials and include the colleague's new field 'has_non_diagnostic_condition'
    trials_df = read_table(spark, jdbc_url, jdbc_props, "silver.trials") \
        .filter((F.col("trial_velocity") >= 0) & (F.col("trial_velocity") < 150)) \
        .cache()

    raw_sites_df = read_table(spark, jdbc_url, jdbc_props, "silver.trial_sites")
    
    # Centralized filter for valid facilities
    sites_df = raw_sites_df.filter(
        F.col("facility_name").isNotNull() & 
        (F.trim(F.col("facility_name")) != "") &
        (F.upper(F.trim(F.col("facility_name"))) != "UNKNOWN FACILITY")
    ).cache()

    # Metrics for reporting
    total_trials = trials_df.count()
    total_sites = sites_df.count()
    null_velocity_count = trials_df.filter(F.col("trial_velocity").isNull()).count()

    # Strict geographical filter for historical aggregations
    sites_with_geo_key = sites_df.filter(
        F.col("country").isNotNull() & F.col("city").isNotNull() & F.col("zip").isNotNull()
    )
    skipped_sites_no_geo_key = total_sites - sites_with_geo_key.count()
    
    # Explode conditions natively in Spark (Kept your efficient Spark implementation)
    exploded_conditions_df = sites_with_geo_key.filter(
        F.col("mesh_conditions_ids").isNotNull()
    ).withColumn("condition_id", F.explode(F.col("mesh_conditions_ids")))
    
    # Join to map trial velocity onto geographical sites
    sites_with_velocity = sites_with_geo_key.join(
        trials_df.select("nct_id", "trial_velocity", "start_date"), 
        on="nct_id", 
        how="left"
    ).withColumn(
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

    print("[INFO]: Truncating historical Gold tables...")
    # Kept database truncation only for tables processed directly inside this job
    truncate_tables(["gold.site_conditions_history", "gold.site_history"])
    
    print("[INFO]: Writing to gold.site_history...")
    site_history_df.write.jdbc(url=jdbc_url, table="gold.site_history", mode="append", properties=jdbc_props)
    
    print("[INFO]: Writing to gold.site_conditions_history...")
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
    
    # Selecting features including the colleague's 'has_non_diagnostic_condition' column
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
        F.col("nct_id"),
        F.col("study_type"),
        F.col("primary_purpose"),
        F.col("lead_sponsor_class"),
        F.col("sex"),
        F.col("phase"),
        F.col("enrollment_count").cast("int"),
        F.col("n_sites").cast("int"),
        F.col("duration_months").cast("double"), 
        F.col("mesh_conditions_ids"),
        F.col("has_non_diagnostic_condition").cast("boolean"), # Merged field preserved
        F.col("avg_site_exp").cast("double"),  
        F.col("avg_site_vel").cast("double"),  
        F.col("target_velocity").cast("double")
    )

    trial_features_rows_written = trial_features_df.count()
    removed_trials_count = total_features_before_filter - trial_features_rows_written

    # --- SINGLE ACTION: OPTIMIZED NATIVE KAFKA STREAM SINK ---
    print("[INFO]: Sending transformed features to Kafka topic...")
    (
        trial_features_df.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_GOLD_FEATURES)
        .option("kafka.producer.acks", "1")
        .option("kafka.batch.size", "65536")
        .option("kafka.lingers.ms", "10")
        .save()
    )
    
    # Comprehensive processing summary logs
    print(
        f"[INFO]: silver_to_gold pipeline logs: trials_read={total_trials} sites_read={total_sites} \n"
        f"[INFO]: sites_skipped(no country/city/zip)={skipped_sites_no_geo_key} \n"
        f"[INFO]: trials_with_null_velocity={null_velocity_count} \n"
        f"[INFO]: site_history_written={site_history_rows_written} \n"
        f"[INFO]: -------------------------------------------------------- \n"
        f"[INFO]: FILTER REPORT FOR ML (gold.trial_features): \n"
        f"[INFO]:   -> Total available trials: {total_features_before_filter} \n"
        f"[INFO]:   -> DISCARDED: {removed_trials_count} \n"
        f"[INFO]:   -> KEPT AND SHIPPED TO KAFKA: {trial_features_rows_written} "
        f"({((trial_features_rows_written/total_features_before_filter)*100):.2f}% of total)"
    )

    print("### [SUCCESS]: silver_to_gold pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()