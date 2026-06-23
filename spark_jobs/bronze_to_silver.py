import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, explode, row_number, lit, expr, round, datediff, to_date, from_json, regexp_replace, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.window import Window

from shared.config import load_dotenv
from shared.kafka import produce_silver_partition_to_kafka

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_BRONZE = os.getenv("KAFKA_TOPIC_BRONZE", "trials.bronze")

TOPIC_SILVER_TRIALS = "trials.silver"
TOPIC_SILVER_SITES = "sites.silver"
TOPIC_GOLD_MESH = "mesh.gold" 

# SCHEMA AGGIORNATO E CORRETTO SUL PAYLOAD REALE
JSON_SCHEMA = StructType([
    StructField("protocolSection", StructType([
        StructField("identificationModule", StructType([
            StructField("nctId", StringType()),
            StructField("briefTitle", StringType())
        ])),
        StructField("descriptionModule", StructType([
            StructField("briefSummary", StringType())
        ])),
        StructField("designModule", StructType([
            StructField("studyType", StringType()),
            StructField("phases", ArrayType(StringType())),
            StructField("designInfo", StructType([
                StructField("primaryPurpose", StringType())
            ])),
            StructField("enrollmentInfo", StructType([
                StructField("count", IntegerType())
            ]))
        ])),
        StructField("statusModule", StructType([
            StructField("overallStatus", StringType()),
            StructField("startDateStruct", StructType([StructField("date", StringType())])),
            StructField("primaryCompletionDateStruct", StructType([
                StructField("date", StringType()),
                StructField("type", StringType())
            ]))
        ])),
        StructField("sponsorCollaboratorsModule", StructType([
            StructField("leadSponsor", StructType([
                StructField("name", StringType()),
                StructField("class", StringType())
            ]))
        ])),
        StructField("eligibilityModule", StructType([
            StructField("sex", StringType()),
            StructField("minimumAge", StringType()),
            StructField("maximumAge", StringType())
        ])),
        StructField("contactsLocationsModule", StructType([
            StructField("locations", ArrayType(StructType([
                StructField("facility", StringType()),
                StructField("city", StringType()),
                StructField("state", StringType()),
                StructField("zip", StringType()),
                StructField("country", StringType()),
                StructField("geoPoint", StructType([
                    StructField("lat", DoubleType()),
                    StructField("lon", DoubleType())
                ]))
            ])))
        ]))
    ])),
    StructField("derivedSection", StructType([
        StructField("conditionBrowseModule", StructType([
            StructField("meshes", ArrayType(StructType([
                StructField("id", StringType()),
                StructField("term", StringType())
            ])))
        ]))
    ]))
])

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

    # 2. Parsing strutturato dell'intero JSON
    parsed_df = raw_df.select(
        col("timestamp").alias("kafka_ts"),
        from_json(col("value").cast("string"), JSON_SCHEMA).alias("data")
    ).filter(col("data.protocolSection.identificationModule.nctId").isNotNull())

    proto = col("data.protocolSection")
    derived = col("data.derivedSection")

    # 3. Deduplicazione nativa sulla Window
    window_spec = Window.partitionBy("nct_id").orderBy(col("kafka_ts").desc())
    deduped_df = parsed_df.select(
        proto.identificationModule.nctId.alias("nct_id"),
        proto.identificationModule.briefTitle.alias("brief_title"),
        proto.descriptionModule.briefSummary.alias("brief_summary"),
        proto.designModule.studyType.alias("study_type"),
        proto.designModule.designInfo.primaryPurpose.alias("primary_purpose"),
        proto.statusModule.overallStatus.alias("overall_status"),
        proto.sponsorCollaboratorsModule.leadSponsor.getItem("class").alias("lead_sponsor_class"),
        proto.designModule.phases[0].alias("phase"),
        proto.designModule.enrollmentInfo.count.alias("enrollment_count"),
        proto.statusModule.startDateStruct.date.alias("start_date_raw"),
        proto.statusModule.primaryCompletionDateStruct.date.alias("completion_date_raw"),
        proto.eligibilityModule.sex.alias("sex"),
        proto.eligibilityModule.minimumAge.alias("minimum_age_raw"),
        proto.eligibilityModule.maximumAge.alias("maximum_age_raw"),
        proto.contactsLocationsModule.locations.alias("locations"),
        derived.conditionBrowseModule.meshes.alias("meshes_struct"),
        col("kafka_ts")
    ).withColumn("row_num", row_number().over(window_spec)) \
     .filter(col("row_num") == 1) \
     .drop("row_num")

    deduped_df.cache()

    # --- 4. TRASFORMAZIONE, FILTRI DI QUALITÀ E COSTRUZIONE TRIALS (SILVER) ---
    # Normalizzazione Date Parziali
    trials_raw_dates = deduped_df.withColumn(
        "start_date",
        expr("""
            CASE 
                WHEN length(start_date_raw) = 7 THEN concat(start_date_raw, '-01')
                WHEN length(start_date_raw) = 4 THEN concat(start_date_raw, '-01-01')
                ELSE coalesce(start_date_raw, '1970-01-01')
            END
        """)
    ).withColumn(
        "primary_completion_date",
        expr("""
            CASE 
                WHEN length(completion_date_raw) = 7 THEN concat(completion_date_raw, '-01')
                WHEN length(completion_date_raw) = 4 THEN concat(completion_date_raw, '-01-01')
                ELSE coalesce(completion_date_raw, '1970-01-01')
            END
        """)
    )

    # Applicazione dei filtri di consistenza logica sui dati core (Evita record spazzatura in Silver)
    trials_filtered = trials_raw_dates.filter(
        (col("start_date") != "1970-01-01") & 
        (col("primary_completion_date") != "1970-01-01") &
        (datediff(to_date(col("primary_completion_date")), to_date(col("start_date"))) > 0) & # Date coerenti
        (col("enrollment_count").isNotNull()) & (col("enrollment_count") > 0) &              # Pazienti reali
        (col("study_type").isNotNull()) & 
        (coalesce(col("phase"), lit("UNKNOWN")) != "UNKNOWN")                                 # Fase nota per ML
    )

    # Funzione di Parsing dell'età
    def parse_age_column(col_name):
        return expr(f"""
            CASE 
                WHEN {col_name} IS NULL THEN NULL
                WHEN lower({col_name}) LIKE '%year%' THEN cast(split({col_name}, ' ')[0] as double)
                WHEN lower({col_name}) LIKE '%month%' THEN cast(split({col_name}, ' ')[0] as double) / 12.0
                WHEN lower({col_name}) LIKE '%week%' THEN cast(split({col_name}, ' ')[0] as double) / 52.17
                WHEN lower({col_name}) LIKE '%day%' THEN cast(split({col_name}, ' ')[0] as double) / 365.25
                ELSE cast(regexp_replace({col_name}, '[^0-9.]', '') as double)
            END
        """)

    trials_df = trials_filtered.withColumn("minimum_age_years", parse_age_column("minimum_age_raw")) \
                               .withColumn("maximum_age_years", parse_age_column("maximum_age_raw"))

    # Calcoli stabili di Duration e Velocity (I filtri a monte garantiscono la sicurezza matematica qui)
    trials_df = trials_df.withColumn(
        "enrollment_duration_months", 
        round(datediff(to_date(col("primary_completion_date")), to_date(col("start_date"))) / 30.44, 2)
    ).withColumn(
        "trial_velocity",
        round(col("enrollment_count") / col("enrollment_duration_months"), 4)
    )
    
    # Ulteriore filtro di sicurezza per escludere outlier estremi di Velocity
    trials_df = trials_df.filter(col("trial_velocity") < 150.0)

    trials_df = trials_df.withColumn("mesh_conditions_ids", expr("transform(meshes_struct, x -> x.id)"))
    trials_df = trials_df.withColumn("phase", coalesce(col("phase"), lit("UNKNOWN")))
    trials_df = trials_df.withColumn("sex", coalesce(col("sex"), lit("ALL")))

    # --- 5. TRASFORMAZIONE E COSTRUZIONE SITES (SILVER) ---
    sites_df = trials_df.filter(col("locations").isNotNull()) \
        .withColumn("loc", explode("locations")) \
        .select(
            col("nct_id"),
            coalesce(col("loc.facility"), lit("UNKNOWN FACILITY")).alias("facility_name"),
            coalesce(col("loc.city"), lit("UNKNOWN CITY")).alias("city"),
            coalesce(col("loc.state"), lit("N/A")).alias("state"),
            coalesce(col("loc.zip"), lit("N/A")).alias("zip"),
            coalesce(col("loc.country"), lit("UNKNOWN")).alias("country"),
            coalesce(col("loc.geoPoint.lat"), lit(0.0)).alias("latitude"),
            coalesce(col("loc.geoPoint.lon"), lit(0.0)).alias("longitude"),
            col("mesh_conditions_ids")
        )

    # --- 6. TRASFORMAZIONE E COSTRUZIONE MESH MAPPINGS (GOLD MESH) ---
    mesh_df = trials_df.filter(col("meshes_struct").isNotNull())\
        .select(explode("meshes_struct").alias("m"))\
        .select(col("m.id").alias("mesh_condition_id"), col("m.term").alias("mesh_condition_name"))\
        .distinct()

    trials_final = trials_df.drop("meshes_struct", "minimum_age_raw", "maximum_age_raw", "start_date_raw", "completion_date_raw", "locations")

    # --- 7. SCRITTURA SU KAFKA ---
    trials_final.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value") \
        .rdd.foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_SILVER_TRIALS))

    sites_df.selectExpr("cast(nct_id as string) as key", "to_json(struct(*)) as value") \
        .rdd.foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_SILVER_SITES))

    mesh_df.selectExpr("cast(mesh_condition_id as string) as key", "to_json(struct(*)) as value") \
        .rdd.foreachPartition(lambda rows: produce_silver_partition_to_kafka(rows, TOPIC_GOLD_MESH))

    print(f"[INFO]: Pipeline Spark Bronze -> Silver completata con successo in modo nativo e filtrata.")
    spark.stop()

if __name__ == "__main__":
    main()