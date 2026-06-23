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

echo "=================================================="
echo "     RUNNING CLINICAL TRIALS PIPELINE (LINUX)     "
echo "=================================================="

# 1. Fetcher Ingestion
echo -e "\n====== [1/3] STARTING INGESTION (FETCHER) ======"
python -m ingestion.fetcher
wait_for_consumer_group "clinical_trials_bronze_loader"

# 2. Spark Bronze to Silver
echo -e "\n====== [2/3] STARTING SPARK JOB: BRONZE TO SILVER ======"
docker exec -it --user root clinical_trial_spark bash -c \
"cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false \
--conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver.py"
wait_for_consumer_group "clinical_trials_silver_relational_loader"

# 3. Spark Silver to Gold
echo -e "\n====== [3/3] STARTING SPARK JOB: SILVER TO GOLD ======"
docker exec -it --user root clinical_trial_spark bash -c \
"cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false \
--conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR \
--packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/silver_to_gold.py"
wait_for_consumer_group "clinical_trials_gold_features_loader"

echo -e "\n=================================================="
echo " [SUCCESS] Data Pipeline executed successfully!   "
echo "=================================================="