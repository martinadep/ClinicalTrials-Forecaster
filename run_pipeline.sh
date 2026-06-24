#!/bin/bash
set -e

MAX_TRIALS_ARG=""
WITH_TRAINING=false
for arg in "$@"; do
  if [ "$arg" = "--with-training" ]; then
    WITH_TRAINING=true
  elif [ -n "$arg" ]; then
    MAX_TRIALS_ARG="--max-trials $arg"
  fi
done

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

# Function to wait for a container's Docker healthcheck to report "healthy"
wait_for_container_healthy() {
  local container_name=$1
  local timeout_seconds=${2:-360}
  local elapsed=0
  echo "[CHECK]: Waiting for container '$container_name' to become healthy..."

  while true; do
    local status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null)

    if [ "$status" = "healthy" ]; then
      echo "[OK]: Container '$container_name' is healthy."
      break
    fi
    if [ "$elapsed" -ge "$timeout_seconds" ]; then
      echo "[WARN]: Timed out waiting for '$container_name' to become healthy (status: $status). Check 'docker logs $container_name'."
      break
    fi

    echo "[WAIT]: '$container_name' status: $status. Retrying in 5 seconds..."
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

if [ "$WITH_TRAINING" = true ]; then
  TOTAL_STEPS=5
else
  TOTAL_STEPS=3
fi

echo "=================================================="
echo "    RUNNING CLINICAL TRIALS PIPELINE (LINUX)     "
echo "=================================================="

# 1. Fetcher Ingestion
echo -e "\n====== [1/$TOTAL_STEPS] STARTING INGESTION (FETCHER) ======"
python -m ingestion.fetcher $MAX_TRIALS_ARG
wait_for_consumer_group "clinical_trials_bronze_loader"

# 2. Spark Bronze to Silver
echo -e "\n====== [2/$TOTAL_STEPS] STARTING SPARK JOB: BRONZE TO SILVER ======"
docker exec -it --user root clinical_trial_spark bash -c \
"cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false \
--conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver.py"
wait_for_consumer_group "clinical_trials_silver_relational_loader"

# 3. Spark Silver to Gold
echo -e "\n====== [3/$TOTAL_STEPS] STARTING SPARK JOB: SILVER TO GOLD ======"
docker exec -it --user root clinical_trial_spark bash -c \
"cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false \
--conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR \
--packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/silver_to_gold.py"
wait_for_consumer_group "clinical_trials_gold_features_loader"

if [ "$WITH_TRAINING" = false ]; then
  echo -e "\n=================================================="
  echo " [SUCCESS] Data Pipeline executed successfully!   "
  echo "=================================================="
else
  # 4. Train the model
  echo -e "\n====== [4/$TOTAL_STEPS] TRAINING THE MODEL ======"
  docker exec -it --user root clinical_trial_spark bash -c \
  "cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR \
  --packages org.postgresql:postgresql:42.7.3 models/train.py"

  # 5. Refresh the model API and dashboard.
  echo -e "\n====== [5/$TOTAL_STEPS] REFRESHING ML API + DASHBOARD ======"
  docker compose up -d --force-recreate ml-api dashboard
  wait_for_container_healthy "clinical_trial_ml_api"
  wait_for_container_healthy "clinical_trial_dashboard"
  echo "[INFO]: Dashboard is up at http://localhost:8501"

  echo -e "\n=================================================="
  echo " [SUCCESS] Pipeline, training, and dashboard refresh complete! "
  echo "=================================================="
fi