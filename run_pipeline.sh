#!/bin/bash
set -e

# Function to wait for a Kafka consumer group to process all pending messages (Lag = 0)
wait_for_consumer_group() {
  local group_name=$1
  echo "[CHECK]: Waiting for consumer group '$group_name' to clear Kafka lag..."
  
  while true; do
    local lag=$(docker exec clinical_trial_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "$group_name" 2>/dev/null | awk 'NR>1 {sum+=$6} END {print sum+0}')
    
    if [ "$lag" -eq 0 ]; then
      echo "[OK]: Consumer group '$group_name' has processed all messages successfully."
      break
    else
      echo "[WAIT]: Queue lag: $lag messages remaining. Checking again in 5 seconds..."
      sleep 5
    fi
  done
}

echo "====== [1/3] STARTING INGESTION (FETCHER) ======"
python -m ingestion.fetcher
wait_for_consumer_group "clinical_trials_bronze_loader"

echo "====== [2/3] STARTING SPARK JOB: BRONZE TO SILVER ======"
docker exec --user root clinical_trial_spark bash -c \
  "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver.py"
wait_for_consumer_group "clinical_trials_silver_relational_loader"

echo "====== [3/3] STARTING SPARK JOB: SILVER TO GOLD ======"
docker exec --user root clinical_trial_spark bash -c \
  "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.3 spark_jobs/silver_to_gold.py"
wait_for_consumer_group "clinical_trials_gold_features_loader"

echo "====== [SUCCESS] Data Pipeline executed completely and successfully! ======"