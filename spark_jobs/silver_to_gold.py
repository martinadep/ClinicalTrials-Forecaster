import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import load_dotenv
from shared.db import build_jdbc_url_from_env , truncate_tables

KAFKA_TOPIC_GOLD_TRIALS = os.getenv("KAFKA_TOPIC_GOLD_TRIALS", "kt.gold.trials")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def read_table(spark, jdbc_url, properties, dbtable):
    """Loads a database table via JDBC."""
    return spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)

def main():
    load_dotenv()
    jdbc_url, jdbc_props = build_jdbc_url_from_env()

    print("[START]: Initializing Spark Session for Silver -> Gold ...")
    
    spark = SparkSession.builder \
        .appName("silver_to_gold") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") 

    print("[INFO]: Loading source tables from Silver relational schema...")
    trials_df = read_table(spark, jdbc_url, jdbc_props, "silver.trials") \
        .filter((F.col("trial_velocity") >= 0) & (F.col("trial_velocity") < 150)) \
        .cache()

    raw_sites_df = read_table(spark, jdbc_url, jdbc_props, "silver.trial_sites")
    
    sites_df = raw_sites_df.filter(
        F.col("facility_name").isNotNull() & 
        (F.trim(F.col("facility_name")) != "") &
        (F.upper(F.trim(F.col("facility_name"))) != "UNKNOWN FACILITY")
    ).cache()

    sites_with_geo_key = sites_df.filter(
        F.col("country").isNotNull() & F.col("city").isNotNull() & F.col("zip").isNotNull()
    )
    
    print("[INFO]: Processing site conditions...")
    exploded_conditions_df = sites_with_geo_key.filter(
        F.col("mesh_conditions_ids").isNotNull()
    ).withColumn("condition_id", F.explode(F.col("mesh_conditions_ids")))
    
    sites_with_velocity = sites_with_geo_key.join(
        trials_df.select("nct_id", "trial_velocity", "start_date"), 
        on="nct_id", 
        how="left"
    ).withColumn(
        "trial_velocity", F.coalesce(F.col("trial_velocity"), F.lit(0.0))
    )

    print("[INFO]: Computing baseline geographical and facility aggregations...")
    
    site_history_df = (
        sites_with_velocity.groupBy("country", "city", "zip")
        .agg(
            F.first("facility_name", ignorenulls=True).alias("facility_name"),
            F.first("state", ignorenulls=True).alias("state"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.first("longitude", ignorenulls=True).alias("longitude"),
            F.count("nct_id").alias("n_trials"), 
            F.avg("trial_velocity").alias("avg_velocity"),
            F.max(F.year("start_date")).alias("last_year"),
        )
        .cache()
    )

    site_conditions_history_df = (
        exploded_conditions_df.groupBy("country", "city", "zip", "condition_id")
        .agg(F.count("nct_id").alias("n_trials_for_condition"))
        .withColumnRenamed("condition_id", "mesh_condition_id")
    )
    
    print("[INFO]: Truncating historical Gold tables inside the DB...")
    truncate_tables(["gold.site_history", "gold.site_conditions_history"]) 
    
    print("[INFO]: -> Writing historical records to gold.site_history...")
    (site_history_df.write
     .mode("append")
     .jdbc(url=jdbc_url, table="gold.site_history", properties=jdbc_props))

    
    print("[INFO]: -> Writing conditions mappings to gold.site_conditions_history...")
    (site_conditions_history_df.select(
        "country", "city", "zip", "mesh_condition_id", "n_trials_for_condition"
    ).write
     .mode("append")
     .jdbc(url=jdbc_url, table="gold.site_conditions_history", properties=jdbc_props))

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
        F.col("has_non_diagnostic_condition").cast("boolean"),
        F.col("avg_site_exp").cast("double"),  
        F.col("avg_site_vel").cast("double"),  
        F.col("target_velocity").cast("double")
    )

    print(f"[INFO]: -> Writing dataframes to Kafka topic: {KAFKA_TOPIC_GOLD_TRIALS}...")
    (
        trial_features_df.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_TOPIC_GOLD_TRIALS)
        .option("kafka.producer.acks", "1")
        .option("kafka.batch.size", "65536")
        .option("kafka.lingers.ms", "10")
        .save()
    )
    
    print("### [SUCCESS]: silver_to_gold pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()