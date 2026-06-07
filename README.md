# ClinicalTrials-Forecaster
Project for BDT


## Running the project

### Requirements

Install:

- Docker
- Docker Compose

### Setup

Create a local environment file:

```bash
cp .env.example .env
```

Edit `.env` and choose a PostgreSQL password.

Example:

```env
POSTGRES_DB=clinical_trials
POSTGRES_USER=postgres
POSTGRES_PASSWORD=admin123
POSTGRES_PORT=5432

KAFKA_BROKER=localhost:9092
KAFKA_TOPIC_BRONZE=trials.bronze
```

### Start the services

Run:

```bash
docker compose up -d
```

The first startup will:

* download the PostgreSQL, Adminer, and Kafka images
* create the database and Kafka containers
* create persistent Docker volumes for both
* execute `db/01_bronze.sql` to initialize the bronze schema
* create the Kafka topics (`trials.bronze`, `trials.silver`, `trials.gold`, `trials.forecasts`)

The services will be available at:

* PostgreSQL: `localhost:5432`
* Adminer: `http://localhost:8080`
* Kafka: `localhost:9092` (from host machine) / `kafka:29092` (from inside Docker network)
* Kafdrop (Kafka UI): `http://localhost:9000`

### Accessing the database

If Adminer is enabled, open:

```text
http://localhost:8080
```

Login using:

* System: `PostgreSQL`
* Server: `postgres`
* Username: value of `POSTGRES_USER`
* Password: value of `POSTGRES_PASSWORD`
* Database: value of `POSTGRES_DB`

### Kafka

Kafka runs in KRaft mode (no Zookeeper). Four topics are pre-created at startup:

| Topic | Purpose |
|-------|---------|
| `trials.bronze` | Raw study payloads from ClinicalTrials.gov |
| `trials.silver` | Cleaned and normalized trial records |
| `trials.gold` | Feature-engineered records ready for ML |
| `trials.forecasts` | ML inference outputs |

Each topic has 3 partitions and replication factor 1 (single-broker setup, suitable for development; production would use ≥3).

#### Inspecting Kafka

List topics:

```bash
docker exec clinical_trial_kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Describe a topic (partitions, leader, replicas):

```bash
docker exec clinical_trial_kafka kafka-topics --bootstrap-server localhost:9092 --topic trials.bronze --describe
```

Check message counts per partition:

```bash
docker exec clinical_trial_kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic trials.bronze
```

Read messages from a topic (Ctrl+C to exit):

```bash
docker exec clinical_trial_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trials.bronze --from-beginning --max-messages 10
```

Kafdrop provides a web UI to browse topics, partitions and messages. Open:

```text
http://localhost:9000
```

Use the Kafdrop UI to inspect `trials.bronze`, `trials.silver`, `trials.gold`, and `trials.forecasts`.

### Resetting the system

To completely recreate the database and Kafka state:

```bash
docker compose down -v
docker compose up -d
```

This removes the existing Docker volumes and reruns initialization. **Warning:** this wipes both Postgres data and Kafka topic data.

### Running testing fetcher

```bash
python -m ingestion.fetcher
```

This will:

* fetch one page of studies from the ClinicalTrials.gov API
* insert them into `bronze.raw_trials` and `bronze.trials` in Postgres
* produce each study payload to the `trials.bronze` Kafka topic

You can verify the results through:

* [Adminer](#accessing-the-database) for the Postgres rows
* Kafka console consumer (see [Inspecting Kafka](#inspecting-kafka)) for the topic messages
