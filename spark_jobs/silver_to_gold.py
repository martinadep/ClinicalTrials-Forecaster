import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import load_dotenv
from shared.db import build_jdbc_url_from_env, truncate_tables
from shared.kafka import produce_silver_partition_to_kafka 

def read_table(spark, jdbc_url, properties, dbtable):
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)

def main():
    load_dotenv()
    jdbc_url, jdbc_props = build_jdbc_url_from_env()
    TOPIC_GOLD_FEATURES = os.getenv("KAFKA_TOPIC_GOLD_FEATURES", "trials.gold")

    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 

    # 1. Caricamento tabelle Silver
    trials_df = read_table(spark, jdbc_url, jdbc_props, "silver.trials").cache()
    raw_sites_df = read_table(spark, jdbc_url, jdbc_props, "silver.trial_sites")
    
    sites_df = raw_sites_df.filter(
        F.col("facility_name").isNotNull() & 
        (F.trim(F.col("facility_name")) != "") &
        (F.upper(F.trim(F.col("facility_name"))) != "UNKNOWN FACILITY")
    ).cache()

    trials_df = trials_df.filter((F.col("trial_velocity") >= 0) & (F.col("trial_velocity") < 150))
    
    # Lettura ottimizzata delle condizioni esplose
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
    
    sites_with_geo_key = sites_df.filter(
        F.col("country").isNotNull() & F.col("city").isNotNull() & F.col("zip").isNotNull()
    )
    
    sites_with_velocity = sites_with_geo_key.join(
        trials_df.select("nct_id", "trial_velocity", "start_date"), on="nct_id", how="left"
    ).withColumn(
        "trial_velocity", F.coalesce(F.col("trial_velocity"), F.lit(0.0))
    )

    # 2. Calcolo aggregati Gold per i Siti
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

    # Tronca e sovrascrive le tabelle storiche aggregate (Essendo tabelle analitiche di snapshot)
    print("[INFO]: Tronco le tabelle storiche dei siti...")
    truncate_tables(["gold.site_conditions_history", "gold.site_history"])
    
    print("[INFO]: Scrittura di gold.site_history...")
    site_history_df.write.jdbc(url=jdbc_url, table="gold.site_history", mode="append", properties=jdbc_props)
    
    print("[INFO]: Scrittura di gold.site_conditions_history...")
    site_conditions_history_df.select(
        "country", "city", "zip", "mesh_condition_id", "n_trials_for_condition"
    ).write.jdbc(url=jdbc_url, table="gold.site_conditions_history", mode="append", properties=jdbc_props)
    
    # 3. Calcolo delle Feature dei Trial (Machine Learning Ready)
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
            "enrollment_count", "enrollment_duration_months", "trial_velocity", "mesh_conditions_ids"
        )
        .withColumnRenamed("enrollment_duration_months", "duration_months")
        .withColumnRenamed("trial_velocity", "target_velocity")
        .join(n_sites_df, on="nct_id", how="left")
        .join(site_stats_per_trial, on="nct_id", how="left")
        .withColumn("n_sites", F.coalesce(F.col("n_sites"), F.lit(0)))
    )
    
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
        F.col("avg_site_exp").cast("double"),  
        F.col("avg_site_vel").cast("double"),  
        F.col("target_velocity").cast("double")
    )

    print(f"[INFO]: Invio feature trasformate al topic Kafka: {TOPIC_GOLD_FEATURES}")
    trial_features_df.foreachPartition(
        lambda rows: produce_silver_partition_to_kafka(rows, topic_name=TOPIC_GOLD_FEATURES)
    )
    
    print("### [SUCCESS]: Pipeline silver_to_gold completata correttamente.")
    spark.stop()

if __name__ == "__main__":
    main()