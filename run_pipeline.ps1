param (
    [int]$MaxTrials = 0,
    [switch]$WithTraining
)

function Wait-ConsumerGroup {
    param ( [string]$GroupName )
    Write-Host "[CHECK]: Waiting for consumer group '$GroupName' to clear Kafka lag..." -ForegroundColor Cyan
    
    while ($true) {
        $output = docker exec clinical_trial_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $GroupName 2>$null
        
        $lag = 0
        if ($output) {
            foreach ($line in $output) {
                if ($line -match "^\s*$" -or $line -contains "GROUP" -or $line -contains "TOPIC") { continue }
                $parts = $line -split '\s+' | Where-Object { $_ -ne "" }
                if ($parts.Count -ge 6) {
                    $partLag = 0
                    if ([int32]::TryParse($parts[5], [ref]$partLag)) {
                        $lag += $partLag
                    }
                }
            }
        }
        
        if ($lag -eq 0) {
            Write-Host "[OK]: Consumer group '$GroupName' has processed all messages." -ForegroundColor Green
            break
        } else {
            Write-Host "[WAIT]: Queue lag: $lag messages remaining. Retrying in 5 seconds..." -ForegroundColor DarkYellow
            Start-Sleep -Seconds 5
        }
    }
}

function Wait-ContainerHealthy {
    param ( [string]$ContainerName, [int]$TimeoutSeconds = 360 )
    Write-Host "[CHECK]: Waiting for container '$ContainerName' to become healthy..." -ForegroundColor Cyan

    $elapsed = 0
    while ($true) {
        $status = docker inspect --format='{{.State.Health.Status}}' $ContainerName 2>$null

        if ($status -eq "healthy") {
            Write-Host "[OK]: Container '$ContainerName' is healthy." -ForegroundColor Green
            break
        }
        if ($elapsed -ge $TimeoutSeconds) {
            Write-Host "[WARN]: Timed out waiting for '$ContainerName' to become healthy (status: $status). Check 'docker logs $ContainerName'." -ForegroundColor Red
            break
        }

        Write-Host "[WAIT]: '$ContainerName' status: $status. Retrying in 5 seconds..." -ForegroundColor DarkYellow
        Start-Sleep -Seconds 5
        $elapsed += 5
    }
}

$TotalSteps = if ($WithTraining) { 5 } else { 3 }

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "     RUNNING CLINICAL TRIALS PIPELINE (WINDOWS)   " -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Fetcher Ingestion
Write-Host "`n====== [1/$TotalSteps] STARTING INGESTION (FETCHER) ======" -ForegroundColor Yellow

$fetcherArgs = @("-m", "ingestion.fetcher")
if ($MaxTrials -gt 0) {
    $fetcherArgs += @("--max-trials", $MaxTrials)
}

python $fetcherArgs
Wait-ConsumerGroup "clinical_trials_bronze_loader"

# 2. Spark Bronze to Silver
Write-Host "`n====== [2/$TotalSteps] STARTING SPARK JOB: BRONZE TO SILVER ======" -ForegroundColor Yellow
docker exec -it --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false --conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver.py"
Wait-ConsumerGroup "clinical_trials_silver_relational_loader"

# 3. Spark Silver to Gold
Write-Host "`n====== [3/$TotalSteps] STARTING SPARK JOB: SILVER TO GOLD ======" -ForegroundColor Yellow
docker exec -it --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false --conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/silver_to_gold.py"
Wait-ConsumerGroup "clinical_trials_gold_features_loader"

if (-not $WithTraining) {
    Write-Host "`n==================================================" -ForegroundColor Green
    Write-Host " [SUCCESS] Data Pipeline executed successfully!   " -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
}
else {
    # 4. Train the model (Docker-routed -- same container/pattern as steps 2-3,
    # avoids the local Windows Hadoop/winutils.exe issue)
    Write-Host "`n====== [4/$TotalSteps] TRAINING THE MODEL ======" -ForegroundColor Yellow
    docker exec -it --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.showConsoleProgress=false --conf spark.driver.extraJavaOptions=-Dlog4j.configurationProcessor=ERROR --packages org.postgresql:postgresql:42.7.3 models/train.py"

    # 5. Refresh the model API and dashboard. --force-recreate is required, not
    # optional: ml-api caches the loaded model in memory for its process
    # lifetime, and dashboard only runs retrieve_data.py once at container
    # startup -- a plain `up -d` is a no-op if they're already running.
    Write-Host "`n====== [5/$TotalSteps] REFRESHING ML API + DASHBOARD ======" -ForegroundColor Yellow
    docker compose up -d --force-recreate ml-api dashboard
    Wait-ContainerHealthy "clinical_trial_ml_api"
    Wait-ContainerHealthy "clinical_trial_dashboard"
    Write-Host "[INFO]: Dashboard is up at http://localhost:8501" -ForegroundColor Cyan

    Write-Host "`n==================================================" -ForegroundColor Green
    Write-Host " [SUCCESS] Pipeline, training, and dashboard refresh complete! " -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
}