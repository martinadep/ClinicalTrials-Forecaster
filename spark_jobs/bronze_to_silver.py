"""
Bronze -> Silver ETL job.

Batch-reads study JSON from Kafka topic `trials.bronze`, cleans/normalizes fields,
writes to Postgres `silver.trials` (upsert by nct_id) and `silver.trial_sites`
(delete+insert per nct_id), and re-publishes the cleaned trial to `trials.silver`.

Run via spark-submit (see docker-compose `spark` service) from the project root so
that `shared.*` imports resolve, e.g.:

    spark-submit --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
        spark_jobs/bronze_to_silver.py
"""
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from shared.config import load_dotenv
from shared.db import build_dsn_from_env
from shared.kafka import build_kafka_producer
from shared.transforms import normalize_date, parse_age_to_years

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_BRONZE = os.getenv("KAFKA_TOPIC_BRONZE", "trials.bronze")
TOPIC_SILVER = os.getenv("KAFKA_TOPIC_SILVER", "trials.silver")

TRIALS_SCHEMA = StructType([
    StructField("nct_id", StringType()),
    StructField("brief_title", StringType()),
    StructField("brief_summary", StringType()),
    StructField("study_type", StringType()),
    StructField("primary_purpose", StringType()),
    StructField("overall_status", StringType()),
    StructField("lead_sponsor_class", StringType()),
    StructField("enrollment_count", IntegerType()),
    StructField("start_date", StringType()),
    StructField("primary_completion_date", StringType()),
    StructField("healthy_volunteers", BooleanType()),
    StructField("sex", StringType()),
    StructField("minimum_age_years", DoubleType()),
    StructField("maximum_age_years", DoubleType()),
    StructField("enrollment_duration_months", DoubleType()),
    StructField("trial_velocity", DoubleType()),
])

SITES_SCHEMA = StructType([
    StructField("nct_id", StringType()),
    StructField("facility_name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("conditions", ArrayType(StringType())),
])

_DAYS_PER_MONTH = 30.44


def _duration_months(start_str, end_str):
    """Months between two YYYY-MM-DD strings, or None if either is missing/invalid/negative."""
    import datetime

    if not start_str or not end_str:
        return None
    try:
        start = datetime.date.fromisoformat(start_str)
        end = datetime.date.fromisoformat(end_str)
    except ValueError:
        return None
    delta_days = (end - start).days
    if delta_days < 0:
        return None
    return round(delta_days / _DAYS_PER_MONTH, 2)


def _trial_velocity(enrollment_count, duration_months):
    """Enrollment rate in patients/month, or None if enrollment or duration is missing/zero."""
    if not enrollment_count or not duration_months:
        return None
    return round(enrollment_count / duration_months, 4)


def parse_study(json_str, kafka_ts):
    """Parse one Kafka message into (nct_id, trial_dict, site_list, kafka_ts), or None if unparseable/no nct_id."""
    try:
        study = json.loads(json_str)
    except (TypeError, ValueError):
        return None

    protocol = study.get("protocolSection", {})
    identification = protocol.get("identificationModule", {})
    nct_id = identification.get("nctId")
    if not nct_id:
        return None

    status = protocol.get("statusModule", {})
    design = protocol.get("designModule", {})
    design_info = design.get("designInfo", {})
    enrollment = design.get("enrollmentInfo", {})
    sponsor = protocol.get("sponsorCollaboratorsModule", {})
    eligibility = protocol.get("eligibilityModule", {})
    description = protocol.get("descriptionModule", {})

    start_date = normalize_date(status.get("startDateStruct", {}).get("date"))
    primary_completion_date = normalize_date(status.get("primaryCompletionDateStruct", {}).get("date"))
    enrollment_count = enrollment.get("count")
    duration_months = _duration_months(start_date, primary_completion_date)

    trial = {
        "nct_id": nct_id,
        "brief_title": identification.get("briefTitle"),
        "brief_summary": description.get("briefSummary"),
        "study_type": design.get("studyType"),
        "primary_purpose": design_info.get("primaryPurpose"),
        "overall_status": status.get("overallStatus"),
        "lead_sponsor_class": sponsor.get("leadSponsor", {}).get("class"),
        "enrollment_count": enrollment_count,
        "start_date": start_date,
        "primary_completion_date": primary_completion_date,
        "healthy_volunteers": eligibility.get("healthyVolunteers"),
        "sex": eligibility.get("sex"),
        "minimum_age_years": parse_age_to_years(eligibility.get("minimumAge")),
        "maximum_age_years": parse_age_to_years(eligibility.get("maximumAge")),
        "enrollment_duration_months": duration_months,
        "trial_velocity": _trial_velocity(enrollment_count, duration_months),
    }

    # Denormalized onto every site row so gold.site_conditions_history can count
    # trials per condition per location without joining back to silver.trials.
    conditions = protocol.get("conditionsModule", {}).get("conditions") or []

    sites = [
        {
            "nct_id": nct_id,
            "facility_name": loc.get("facility"),
            "city": loc.get("city"),
            "state": loc.get("state"),
            "zip": loc.get("zip"),
            "country": loc.get("country"),
            "latitude": loc.get("geoPoint", {}).get("lat"),
            "longitude": loc.get("geoPoint", {}).get("lon"),
            "conditions": conditions,
        }
        for loc in protocol.get("contactsLocationsModule", {}).get("locations", [])
    ]

    return {"nct_id": nct_id, "trial": trial, "sites": sites, "kafka_ts": kafka_ts}


def upsert_trials_partition(rows):
    """Upsert a partition of trial dicts into silver.trials (latest write wins per nct_id)."""
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
                    INSERT INTO silver.trials (
                        nct_id, brief_title, brief_summary, study_type, primary_purpose,
                        overall_status, lead_sponsor_class, enrollment_count, start_date,
                        primary_completion_date, healthy_volunteers, sex,
                        minimum_age_years, maximum_age_years, enrollment_duration_months,
                        trial_velocity, transformed_at
                    ) VALUES %s
                    ON CONFLICT (nct_id) DO UPDATE SET
                        brief_title = EXCLUDED.brief_title,
                        brief_summary = EXCLUDED.brief_summary,
                        study_type = EXCLUDED.study_type,
                        primary_purpose = EXCLUDED.primary_purpose,
                        overall_status = EXCLUDED.overall_status,
                        lead_sponsor_class = EXCLUDED.lead_sponsor_class,
                        enrollment_count = EXCLUDED.enrollment_count,
                        start_date = EXCLUDED.start_date,
                        primary_completion_date = EXCLUDED.primary_completion_date,
                        healthy_volunteers = EXCLUDED.healthy_volunteers,
                        sex = EXCLUDED.sex,
                        minimum_age_years = EXCLUDED.minimum_age_years,
                        maximum_age_years = EXCLUDED.maximum_age_years,
                        enrollment_duration_months = EXCLUDED.enrollment_duration_months,
                        trial_velocity = EXCLUDED.trial_velocity,
                        transformed_at = EXCLUDED.transformed_at
                    """,
                    [
                        (
                            r.nct_id, r.brief_title, r.brief_summary, r.study_type, r.primary_purpose,
                            r.overall_status, r.lead_sponsor_class, r.enrollment_count, r.start_date,
                            r.primary_completion_date, r.healthy_volunteers, r.sex,
                            r.minimum_age_years, r.maximum_age_years, r.enrollment_duration_months,
                            r.trial_velocity,
                        )
                        for r in rows
                    ],
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                )
    finally:
        conn.close()


def insert_sites_partition(rows):
    """Insert a partition of site dicts into silver.trial_sites (existing rows for these nct_ids already cleared)."""
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
                    INSERT INTO silver.trial_sites (
                        nct_id, facility_name, city, state, zip, country,
                        latitude, longitude, conditions, transformed_at
                    )
                    VALUES %s
                    """,
                    [
                        (
                            r.nct_id, r.facility_name, r.city, r.state, r.zip, r.country,
                            r.latitude, r.longitude, r.conditions,
                        )
                        for r in rows
                    ],
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                )
    finally:
        conn.close()


def delete_existing_sites(nct_ids):
    """Delete existing silver.trial_sites rows for the given nct_ids (run once, driver-side, before inserting)."""
    if not nct_ids:
        return
    import psycopg2

    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM silver.trial_sites WHERE nct_id = ANY(%s)",
                    (list(nct_ids),),
                )
    finally:
        conn.close()


def produce_to_silver_topic(trial_dicts):
    """Publish each cleaned trial dict to trials.silver, keyed by nct_id."""
    producer = build_kafka_producer()
    if producer is None:
        return
    for trial in trial_dicts:
        nct_id = trial["nct_id"]
        producer.produce(
            TOPIC_SILVER,
            key=nct_id.encode("utf-8"),
            value=json.dumps(trial, ensure_ascii=False).encode("utf-8"),
        )
    producer.flush()


def main():
    load_dotenv()
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_BRONZE)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    total_messages = kafka_df.count()

    parsed_rdd = (
        kafka_df.select(col("value").cast("string").alias("json_str"), col("timestamp").alias("kafka_ts"))
        .rdd.map(lambda row: parse_study(row.json_str, row.kafka_ts))
        .filter(lambda parsed: parsed is not None)
        .cache()
    )

    # Dedup: one (trial, sites) per nct_id, keeping the message with the latest Kafka timestamp.
    deduped_rdd = parsed_rdd.keyBy(lambda parsed: parsed["nct_id"]).reduceByKey(
        lambda a, b: a if a["kafka_ts"] >= b["kafka_ts"] else b
    )
    parsed_ok = deduped_rdd.count()
    skipped = total_messages - parsed_rdd.count()

    trials_df = spark.createDataFrame(deduped_rdd.map(lambda kv: kv[1]["trial"]), schema=TRIALS_SCHEMA)
    sites_df = spark.createDataFrame(deduped_rdd.flatMap(lambda kv: kv[1]["sites"]), schema=SITES_SCHEMA)

    nct_ids_in_batch = [row.nct_id for row in trials_df.select("nct_id").collect()]
    delete_existing_sites(nct_ids_in_batch)

    trials_df.foreachPartition(upsert_trials_partition)
    sites_df.foreachPartition(insert_sites_partition)

    trial_dicts = [row.asDict() for row in trials_df.collect()]
    produce_to_silver_topic(trial_dicts)

    print(
        f"[INFO]: bronze_to_silver: processed={total_messages} written={parsed_ok} "
        f"skipped(no nct_id/unparseable)={skipped}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
