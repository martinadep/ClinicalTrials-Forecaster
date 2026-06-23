import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, explode, row_number, lit, expr, round, datediff, to_date
from pyspark.sql.window import Window

from shared.config import load_dotenv
from shared.kafka import produce_silver_partition_to_kafka

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_BRONZE = os.getenv("KAFKA_TOPIC_BRONZE", "trials.bronze")

TOPIC_SILVER_TRIALS = "trials.silver"
TOPIC_SILVER_SITES = "sites.silver"
TOPIC_GOLD_MESH = "mesh.gold" 

def main():
    load_dotenv()
    spark = SparkSession.builder \
        .appName("bronze_to_silver_native") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Lettura da Kafka Bronze
    raw_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_BRONZE)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # 2. Stringifica il valore ed estrai l'NCT_ID iniziale per la deduplica
    df_with_id = raw_df.select(
        col("value").cast("string").alias("json_str"),
        col("timestamp").alias("kafka_ts"),
        expr("get_json_object(string(value), '$.protocolSection.identificationModule.nctId')").alias("nct_id")
    ).filter(col("nct_id").isNotNull())

    # 3. Deduplicazione nativa
    window_spec = Window.partitionBy("nct_id").orderBy(col("kafka_ts").desc())
    deduped_df = df_with_id.withColumn("row_num", row_number().over(window_spec)) \
                           .filter(col("row_num") == 1) \
                           .drop("row_num")

    deduped_df.cache()

    # --- 4. TRASFORMAZIONE E COSTRUZIONE TRIALS (SILVER) ---
    trials_df = deduped_df.select(
        col("nct_id"),
        expr("get_json_object(json_str, '$.protocolSection.identificationModule.briefTitle')").alias("brief_title"),
        expr("get_json_object(json_str, '$.protocolSection.descriptionModule.briefSummary')").alias("brief_summary"),
        expr("upper(get_json_object(json_str, '$.protocolSection.designModule.studyType'))").alias("study_type"),
        expr("upper(get_json_object(json_str, '$.protocolSection.designModule.designInfo.primaryPurpose'))").alias("primary_purpose"),
        expr("upper(get_json_object(json_str, '$.protocolSection.statusModule.overallStatus'))").alias("overall_status"),
        expr("upper(get_json_object(json_str, '$.protocolSection.sponsorCollaboratorsModule.leadSponsor.class'))").alias("lead_sponsor_class"),
        expr("upper(coalesce(get_json_object(json_str, '$.protocolSection.designModule.phases[0]'), 'UNKNOWN'))").alias("phase"),
        expr("cast(get_json_object(json_str, '$.protocolSection.designModule.enrollmentInfo.count') as int)").alias("enrollment_count"),
        
        # <<< MODIFICA APPLICATA QUI: NORMALIZZAZIONE DELLE DATE PARZIALI >>>
        expr("""
            CASE 
                WHEN length(get_json_object(json_str, '$.protocolSection.statusModule.startDateStruct.date')) = 7 
                    THEN concat(get_json_object(json_str, '$.protocolSection.statusModule.startDateStruct.date'), '-01')
                WHEN length(get_json_object(json_str, '$.protocolSection.statusModule.startDateStruct.date')) = 4 
                    THEN concat(get_json_object(json_str, '$.protocolSection.statusModule.startDateStruct.date'), '-01-01')
                ELSE coalesce(get_json_object(json_str, '$.protocolSection.statusModule.startDateStruct.date'), '1970-01-01')
            END
        """).alias("start_date"),
        
        expr("""
            CASE 
                WHEN length(get_json_object(json_str, '$.protocolSection.statusModule.primaryCompletionDateStruct.date')) = 7 
                    THEN concat(get_json_object(json_str, '$.protocolSection.statusModule.primaryCompletionDateStruct.date'), '-01')
                WHEN length(get_json_object(json_str, '$.protocolSection.statusModule.primaryCompletionDateStruct.date')) = 4 
                    THEN concat(get_json_object(json_str, '$.protocolSection.statusModule.primaryCompletionDateStruct.date'), '-01-01')
                ELSE coalesce(get_json_object(json_str, '$.protocolSection.statusModule.primaryCompletionDateStruct.date'), '1970-01-01')
            END
        """).alias("primary_completion_date"),
        
        expr("upper(coalesce(get_json_object(json_str, '$.protocolSection.eligibilityModule.sex'), 'ALL'))").alias("sex"),
        expr("get_json_object(json_str, '$.protocolSection.eligibilityModule.minimumAge')").alias("minimum_age_raw"),
        expr("get_json_object(json_str, '$.protocolSection.eligibilityModule.maximumAge')").alias("maximum_age_raw"),
        expr("from_json(get_json_object(json_str, '$.derivedSection.conditionBrowseModule.meshes'), 'array<struct<id:string,term:string>>')").alias("meshes_struct")
    )

    # Calcoli di Velocity e Duration
    trials_df = trials_df.withColumn(
        "enrollment_duration_months", 
        round(expr("case when start_date = '1970-01-01' or primary_completion_date = '1970-01-01' then 0.0 else case when datediff(to_date(primary_completion_date), to_date(start_date)) < 0 then 0.0 else datediff(to_date(primary_completion_date), to_date(start_date)) / 30.44 end end"), 2)
    )
    
    trials_df = trials_df.withColumn(
        "trial_velocity",
        round(expr("case when enrollment_count is null or enrollment_duration_months <= 0 then 0.0 else enrollment_count / enrollment_duration_months end"), 4)
    )
    
    trials_df = trials_df.withColumn("mesh_conditions_ids", expr("transform(meshes_struct, x -> x.id)"))

    # --- 5. TRASFORMAZIONE E COSTRUZIONE SITES (SILVER) ---
    sites_raw = deduped_df.select(
        col("nct_id"),
        expr("from_json(get_json_object(json_str, '$.protocolSection.contactsLocationsModule.locations'), 'array<struct<facility:string,city:string,state:string,zip:string,country:string,geoPoint:struct<lat:double,lon:double>>>')").alias("locs"),
        expr("from_json(get_json_object(json_str, '$.derivedSection.conditionBrowseModule.meshes'), 'array<struct<id:string,term:string>>')").alias("meshes_struct")
    ).withColumn("loc", explode("locs"))

    sites_df = sites_raw.select(
        col("nct_id"),
        coalesce(col("loc.facility"), lit("UNKNOWN FACILITY")).alias("facility_name"),
        coalesce(col("loc.city"), lit("UNKNOWN CITY")).alias("city"),
        coalesce(col("loc.state"), lit("N/A")).alias("state"),
        coalesce(col("loc.zip"), lit("N/A")).alias("zip"),
        coalesce(col("loc.country"), lit("UNKNOWN")).alias("country"),
        coalesce(col("loc.geoPoint.lat"), lit(0.0)).alias("latitude"),
        coalesce(col("loc.geoPoint.lon"), lit(0.0)).alias("longitude"),
        expr("transform(meshes_struct, x -> x.id)").alias("mesh_conditions_ids")
    )

    # --- 6. TRASFORMAZIONE E COSTRUZIONE MESH MAPPINGS (GOLD MESH) ---
    mesh_df = trials_df.filter(col("meshes_struct").isNotNull())\
        .select(explode("meshes_struct").alias("m"))\
        .select(col("m.id").alias("mesh_condition_id"), col("m.term").alias("mesh_condition_name"))\
        .distinct()

    trials_final = trials_df.drop("meshes_struct", "minimum_age_raw", "maximum_age_raw")

    # --- 7. SCRITTURA SU KAFKA ---
    trials_final.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value") \
        .foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_SILVER_TRIALS))

    sites_df.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value") \
        .foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_SILVER_SITES))

    mesh_df.selectExpr("cast(mesh_condition_id as string) as key", "to_json(struct(*)) as value") \
        .foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_GOLD_MESH))

    print(f"[INFO]: Pipeline Spark Bronze -> Silver completata con successo in modo nativo.")
    spark.stop()

if __name__ == "__main__":
    main()