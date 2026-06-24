# Clinical Trial Site Selection & Recruitment Forecasting

An Big Data platform designed to help researchers and trial sponsors optimize study planning. The system predicts and ranks candidate sites or regions by their expected **recruitment velocity** (patients enrolled per month), directly addressing patient recruitment delays—one of the primary causes of failure and high costs in clinical trials.

The architecture is fully containerized and processes real-world, heterogeneous data from the **ClinicalTrials.gov API** through an advanced pipeline:

*   **Data Streaming & Ingestion:** Batch ingestion handled by a Python producer and decoupled via **Apache Kafka** topics.
*   **Storage & Refinement:** A **PostgreSQL Medallion Architecture** that structures raw nested JSON into clear, analytics-ready tables (Bronze, Silver, Gold layers).
*   **Distributed Processing:** **Apache Spark** jobs for complex data cleaning and feature engineering.
*   **Predictive Modeling:** A **Spark MLlib Gradient-Boosted Trees (GBT)** regression model that captures trial-specific dynamics and site histories, explaining ~58% of velocity variance ($R^2 \approx 0.58$).
*   **Interactive Serving:** A user-friendly **Streamlit Dashboard** providing researchers with ranked recommendations, key performance metrics, and geographic site mapping.


## Running the project

### Requirements

Install:
- Docker
- Docker Compose
- Python 3.10+ (a virtual environment like `venv` is highly recommended)

### Setup
(Optional) Create virtual environment.

* Linux / macOS:
```bash
pip -m venv .venv
source .venv/bin/activate
```

* Windows (Powershell):
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install python requirements:
```bash
pip install requests confluent-kafka python-dotenv
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
KAFKA_TOPIC_BRONZE_TRIALS=kt.bronze.trials
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
* Auto-create the Kafka topics (`kt.bronze.trials`, `kt.silver.trials`, `kt.silver.sites`, `kt.gold.trials` and `kt.gold.mesh`)

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

> **Training note:** by default the script only runs ingestion + ETL (steps below in section 3 are manual). Add `-WithTraining` (PowerShell) / `--with-training` (Bash) to also train the model and refresh the dashboard as part of the same run -- see section 3 for what that does and why it's opt-in:
> ```powershell
> .\run_pipeline.ps1 -MaxTrials 2000 -WithTraining
> ```
> ```bash
> ./run_pipeline.sh 2000 --with-training
> ```
> With `-WithTraining`, the script doesn't exit as soon as the containers are *started* -- it polls `ml-api` and `dashboard`'s Docker healthchecks and only finishes once both report `healthy`, since neither is actually ready to serve immediately (see the note in Step B below). This can take several minutes.

---

## 3. Machine Learning & Dashboard

Once the processing steps finish and refined analytical inputs populate the data ecosystem, proceed with training and serving components. (Skip this whole section if you ran the pipeline with `-WithTraining`/`--with-training` -- it already did steps A and B below for you.)

Retraining is **not** automatic otherwise -- nothing currently triggers it on its own, by design, so you decide when a retrain is worth the time:

### Step A: Train the Machine Learning Model

Training runs Spark, so like the bronze→silver and silver→gold jobs it must run
inside the `spark` container, not as a bare local `python` command (running it
locally requires a local Hadoop/`winutils.exe` install on Windows and will fail
otherwise):
```bash
docker exec -it --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.3 models/train.py"
```

### Step B: Launch the Model API and Dashboard

The trained model is served by a small FastAPI service (`ml-api`), and the
Streamlit dashboard (`dashboard`) calls it over HTTP -- both run as Docker
Compose services, so no local Python/Spark setup is needed on your machine:
```bash
docker compose up -d --force-recreate ml-api dashboard
```

`--force-recreate` matters here, not just `up -d`: `ml-api` keeps the trained
model loaded in memory for as long as its process runs, and `dashboard` only
runs `dashboard/retrieve_data.py` once, at container startup. If both
containers were already running (e.g. you brought up the full stack with a
plain `docker compose up -d` in section 1), a plain `up -d` here is a no-op
and you'd keep serving the previous model/stale CSVs -- `--force-recreate`
restarts them so the new model and gold-layer data actually get picked up.

Neither container is a no-cache build, so this `pip install`s `ml-api`'s and
`dashboard`'s dependencies from scratch every time -- a container showing
`Up` in `docker compose ps` doesn't mean it's actually ready yet. Both
services have a Docker healthcheck (`ml-api` polls its own `/health`,
`dashboard` polls Streamlit's `/_stcore/health`); wait for `docker compose ps`
to show `(healthy)` next to both before opening the dashboard. The first run
after a fresh recreate has taken up to ~5 minutes in testing; later requests
are fast.

The dashboard UI will then be reachable at `http://localhost:8501`.

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
| `kt.bronze.trials` | Raw study JSON payloads directly from ClinicalTrials.gov |
| `kt.silver.trials` | Cleaned, un-nested, and normalized relational trial records |
|  `kt.silver.sites` | Facilities extracted and exploded from the raw trial payloads |
|  `kt.gold.trials`  | Final engineered features for machine learning, containing trial attributes, aggregated historical site experience, and the target velocity |
|   `kt.gold.mesh`   | Catalog containing distinct MeSH (Medical Subject Headings) condition IDs and their corresponding official names |

#### Inspecting Kafka via CLI

List topics:
```bash
docker exec clinical_trial_kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Read real-time messages from a topic (Ctrl+C to exit):
```bash
docker exec clinical_trial_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic kt.bronze.trials --from-beginning --max-messages 10
```

Alternatively, you can monitor topics, partition configurations, offsets, and consumer group lags via the **Kafdrop Web UI** at `http://localhost:9000`.

