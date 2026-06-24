# ClinicalTrials-Forecaster
Project for BDT (Big Data Technologies)


## Running the project

### Requirements

Install:
- Docker
- Docker Compose
- Python 3.10+ (a virtual environment like `venv` is highly recommended)

### Setup
Install python requirements:
```bash
pip install -r requirements.txt
```

Create a local environment file:
```bash
cp .env.example .env
```

Edit `.env` and configure your credentials and setup. Example:
```env
POSTGRES_DB=clinical_trials
POSTGRES_USER=postgres
POSTGRES_PASSWORD=admin123
POSTGRES_PORT=5432

KAFKA_BROKER=localhost:9092
KAFKA_TOPIC_BRONZE=trials.bronze
```

---

## 1. Start the Infrastructure (Docker)

Start Docker Desktop on Windows / macOS:
```bash
docker desktop start
```

To ensure a completely clean state (highly recommended for a clean demo) and spin up the architecture containers, run:
```bash
docker compose down -v
docker compose up -d
```

The first startup will:

* Download and run PostgreSQL, Adminer, Kafka, Kafdrop, and Apache Spark images
* Create persistent Docker volumes for databases and streaming states
* Execute `db/01_bronze.sql`, `db/02_silver.sql` and `db/03_gold.sql` to initialize the database schemas
* Auto-create the Kafka topics (`trials.bronze`, `trials.silver`, `sites.silver`, `trials.gold` and `mesh.gold`)

The infrastructure services will be available at:

* **PostgreSQL:** `localhost:5432`
* **Adminer (DB Web UI):** `http://localhost:8080`
* **Kafka Broker:** `localhost:9092`
* **Kafdrop (Kafka UI):** `http://localhost:9000`

---

## 2. Execute the Data Pipeline (Ingestion & Processing)

The orchestration pipeline handles execution sequentially. It launches the Python Fetcher to ingest live API data into Kafka, followed by Apache Spark jobs that progressively transition and transform data across layers, waiting dynamically until the Kafka consumer lags are completely cleared.

Execute the pipeline script depending on your OS:

### On Windows (PowerShell)
```powershell
.\run_pipeline.ps1 -MaxTrials 2000
```

### On Linux / macOS (Bash)
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh 2000
```

> **Configuration Note:** If you do not provide any argument (e.g., executing simply `.\run_pipeline.ps1`), the system automatically defaults to **15000** trials. Passing a lower value like `2000` is perfect for fast tests or active live demonstrations.

---

## 3. Machine Learning & Dashboard

Once the processing steps finish and refined analytical inputs populate the data ecosystem, proceed with training and serving components.

### Step A: Train the Machine Learning Model

Run the Python task to train the forecaster model on top of your engineered features:
```bash
python -m models.train
```

### Step B: Retrieve Analytics Data

Extract and prepare the predictions alongside real metrics to make them available for the front-end layer:
```bash
python -m dashboard.retrieve_data
```

### Step C: Launch the Streamlit Dashboard

Launch the web interface application to visualize statistics, performance tracking, and forecasts:
```bash
streamlit run dashboard/app.py
```

The client UI will immediately open and become reachable at `http://localhost:8501`.

---

## Technical Appendix

### Accessing the Database

Open `http://localhost:8080` and log in using:

* **System:** `PostgreSQL`
* **Server:** `postgres`
* **Username:** value of `POSTGRES_USER`
* **Password:** value of `POSTGRES_PASSWORD`
* **Database:** value of `POSTGRES_DB`

### Kafka Architecture

Kafka runs in KRaft mode (no Zookeeper). Data transits dynamically across 4 pre-configured topics (each with 3 partitions and a replication factor of 1):

| Topic | Purpose |
| --- | --- |
| `trials.bronze` | Raw study JSON payloads directly from ClinicalTrials.gov |
| `trials.silver` | Cleaned, un-nested, and normalized relational trial records |
| `trials.gold` | Feature-engineered datasets structured ready for ML |
| `trials.forecasts` | ML inference engine outputs and predictions |

#### Inspecting Kafka via CLI

List topics:
```bash
docker exec clinical_trial_kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Read real-time messages from a topic (Ctrl+C to exit):
```bash
docker exec clinical_trial_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trials.bronze --from-beginning --max-messages 10
```

Alternatively, you can monitor topics, partition configurations, offsets, and consumer group lags via the **Kafdrop Web UI** at `http://localhost:9000`.

