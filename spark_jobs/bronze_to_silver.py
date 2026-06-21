import json
import os
import datetime
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
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
    StructField("sex", StringType()),
    StructField("minimum_age_years", DoubleType()),
    StructField("maximum_age_years", DoubleType()),
    StructField("enrollment_duration_months", DoubleType()),
    StructField("trial_velocity", DoubleType()),
    StructField("phase", StringType()), 
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

# Global reference point for executor initializations
_GLOBAL_RULES_BROADCAST = None


def _duration_months(start_str, end_str):
    if not start_str or not end_str or start_str == "1970-01-01" or end_str == "1970-01-01":
        return 0.0
    try:
        start = datetime.date.fromisoformat(start_str)
        end = datetime.date.fromisoformat(end_str)
        delta_days = (end - start).days
        return round(max(0, delta_days) / _DAYS_PER_MONTH, 2)
    except ValueError:
        return 0.0


def _trial_velocity(enrollment_count, duration_months):
    if not enrollment_count or not duration_months or duration_months <= 0:
        return 0.0
    return round(enrollment_count / duration_months, 4)


def _apply_regex_mapping(condition_name, rules):
    """Normalizes variation text strings using regex rules maps."""
    if not condition_name:
        return "GENERAL"
        
    # 1. Convert to string, force Uppercase, strip whitespace
    c_clean = str(condition_name).strip().upper()
    
    # 2. Strip quotes, brackets, and literal database formatting characters
    c_clean = re.sub(r'[\"\'\[\]\(\)]', '', c_clean)
    
    # 3. Replace hyphens, underscores, commas, and punctuation with a single space
    c_clean = re.sub(r'[\-_\,\.\;\:]', ' ', c_clean)
    
    # 4. Collapse multiple spaces into a single uniform whitespace
    c_clean = re.sub(r'\s+', ' ', c_clean).strip()
    if not rules:
        return c_clean.title()
        
    # Execute regex sequence scans
    for rule in rules:
        category = rule["category"]
        if any(pattern.search(c_clean) for pattern in rule["patterns"]):
            return category
            
    return c_clean.title()


def parse_study(json_str, kafka_ts):
    """Parses raw bronze trial JSON records cleanly safely inside spark executors."""
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

    start_date = normalize_date(status.get("startDateStruct", {}).get("date")) or "1970-01-01"
    primary_completion_date = normalize_date(status.get("primaryCompletionDateStruct", {}).get("date")) or "1970-01-01"
    
    enrollment_count = int(enrollment.get("count") or 0)
    duration_months = _duration_months(start_date, primary_completion_date)

    trial = {
        "nct_id": nct_id,
        "brief_title": identification.get("briefTitle") or "UNKNOWN TITLE",
        "brief_summary": description.get("briefSummary") or "NO SUMMARY",
        "study_type": (design.get("studyType") or "UNKNOWN").upper(),
        "primary_purpose": (design_info.get("primaryPurpose") or "UNKNOWN").upper(),
        "overall_status": (status.get("overallStatus") or "UNKNOWN").upper(),
        "lead_sponsor_class": (sponsor.get("leadSponsor", {}).get("class") or "UNKNOWN").upper(),
        "enrollment_count": enrollment_count,
        "start_date": start_date,
        "primary_completion_date": primary_completion_date,
        "sex": (eligibility.get("sex") or "ALL").upper(),
        "minimum_age_years": float(parse_age_to_years(eligibility.get("minimumAge")) or 0.0),
        "maximum_age_years": float(parse_age_to_years(eligibility.get("maximumAge")) or 100.0),
        "enrollment_duration_months": duration_months,
        "trial_velocity": _trial_velocity(enrollment_count, duration_months),
        "phase": (design.get("phases", ["UNKNOWN"])[0] if design.get("phases") else "UNKNOWN").upper()
    }

    # Extract conditions using rules unpacked from distributed Broadcast context
    raw_conditions = protocol.get("conditionsModule", {}).get("conditions") or []
    cleaned_conditions = []
    
    # Safely unpack reference points inside Spark executor worker threads
    rules_to_use = _GLOBAL_RULES_BROADCAST.value if _GLOBAL_RULES_BROADCAST else []

    for c in raw_conditions:
        if not c:
            continue
        
        # Guard filters against systemic structural noise or description blocks
        c_str = str(c).strip()
        if len(c_str) < 2 or len(c_str) > 120:
            continue
            
        c_mapped = _apply_regex_mapping(c_str, rules_to_use)
        
        # Standard filter limits
        if len(c_mapped) < 2 or len(c_mapped) > 75:
            continue
            
        if c_mapped not in cleaned_conditions:
            cleaned_conditions.append(c_mapped)

    if not cleaned_conditions:
        cleaned_conditions = ["GENERAL"]

    sites = [
        {
            "nct_id": nct_id,
            "facility_name": loc.get("facility") or "UNKNOWN FACILITY",
            "city": loc.get("city") or "UNKNOWN CITY",
            "state": loc.get("state") or "N/A",
            "zip": loc.get("zip") or "N/A",
            "country": loc.get("country") or "UNKNOWN",
            "latitude": float(loc.get("geoPoint", {}).get("lat") or 0.0),
            "longitude": float(loc.get("geoPoint", {}).get("lon") or 0.0),
            "conditions": cleaned_conditions,
        }
        for loc in protocol.get("contactsLocationsModule", {}).get("locations", [])
    ]

    return {"nct_id": nct_id, "trial": trial, "sites": sites, "kafka_ts": kafka_ts}


def upsert_trials_partition(rows):
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
                        primary_completion_date, sex, minimum_age_years, maximum_age_years, 
                        enrollment_duration_months, trial_velocity, phase, transformed_at
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
                        sex = EXCLUDED.sex,
                        minimum_age_years = EXCLUDED.minimum_age_years,
                        maximum_age_years = EXCLUDED.maximum_age_years,
                        enrollment_duration_months = EXCLUDED.enrollment_duration_months,
                        trial_velocity = EXCLUDED.trial_velocity,
                        phase = EXCLUDED.phase,
                        transformed_at = EXCLUDED.transformed_at
                    """,
                    [
                        (
                            r.nct_id, r.brief_title, r.brief_summary, r.study_type, r.primary_purpose,
                            r.overall_status, r.lead_sponsor_class, r.enrollment_count, r.start_date,
                            r.primary_completion_date, r.sex, r.minimum_age_years, r.maximum_age_years, 
                            r.enrollment_duration_months, r.trial_velocity, r.phase,
                        )
                        for r in rows
                    ],
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                )
    finally:
        conn.close()


def insert_sites_partition(rows):
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
    global _GLOBAL_RULES_BROADCAST
    load_dotenv()
    
    compiled_rules = []
    mapping_path = "spark_jobs/mapping_rules.json"
    
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, "r", encoding="utf-8") as f:
                raw_rules = json.load(f)
            for r in raw_rules:
                compiled_rules.append({
                    "category": r["category"],
                    "patterns": [re.compile(p, re.IGNORECASE) for p in r["patterns"]]
                })
            print(f"[INFO]: Compiled {len(compiled_rules)} string mapping groups.")
        except Exception as e:
            print(f"[ERROR]: Error parsing mapping_rules.json: {e}")
    else:
        print(f"[WARNING]: Mapping file {mapping_path} missing.")

    # Run Spark Session
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") 

    # Broadicast globally to preserve reference visibility inside the tasks
    _GLOBAL_RULES_BROADCAST = spark.sparkContext.broadcast(compiled_rules)

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_BRONZE)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    total_messages = kafka_df.count()

    # RDD processing chain cleanly references internal module variables on the workers
    parsed_rdd = (
        kafka_df.select(col("value").cast("string").alias("json_str"), col("timestamp").alias("kafka_ts"))
        .rdd.map(lambda row: parse_study(row.json_str, row.kafka_ts))
        .filter(lambda parsed: parsed is not None)
        .cache()
    )

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
        f"[INFO]: Processing completed: total={total_messages} saved={parsed_ok} skipped={skipped}"
    )

    _GLOBAL_RULES_BROADCAST.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()