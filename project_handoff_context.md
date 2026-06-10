# AIOps Distributed Log Diagnostics — Project Context & Handoff

This document provides a comprehensive summary of the current state of the AIOps platform. It is designed to provide full context to any LLM or developer continuing the work.

## 1. Project Overview
The project is a fully containerized, distributed **AIOps Lakehouse & Streaming Architecture** that ingests logs, structures them, applies unsupervised ML for anomaly detection, stores them in a Delta Lakehouse (MinIO) + PostgreSQL, and serves real-time alerts via FastAPI WebSockets and Grafana.

---

## 2. What Has Been Completed (Phase 0 & Phase 1)

We have fully implemented the foundational infrastructure and ingestion pipeline according to the specifications in `read.md`. 

### Infrastructure & Orchestration (`docker-compose.yml`)
- **Network Isolation:** Created 3 isolated bridge networks (`ingestion-net`, `storage-net`, `serving-net`).
- **Kafka Cluster:** Deployed a 3-broker KRaft Kafka cluster (no ZooKeeper) spanning all networks.
- **Storage Layer:** Deployed MinIO (Object Storage) and PostgreSQL (Analytics DB).
- **Compute Layer:** Deployed Spark Master and Worker nodes.
- **Serving Layer:** Deployed FastAPI (Gateway) and Grafana/Prometheus (Dashboards/Metrics).

### Ingestion Pipeline
- **Log Generator (`docker/log-generator/`)**: A Python script running in a container that replays the BGL dataset into a shared Docker volume (`/var/log/aiops/system.log`) at 0.001s intervals to simulate live logs.
- **Fluentd Aggregator (`docker/fluentd/`)**: Tails the shared log file, parses the BGL logs using a regex pipeline (`fluent.conf`), buffers chunks on disk, and forwards them to Kafka.

### Scripts & Configuration
- **Dataset Setup:** `scripts/download_datasets.sh` downloads the Loghub BGL/HDFS datasets and generates synthetic logs.
- **Kafka Topics:** `scripts/setup_kafka_topics.py` creates `system-logs` (3 partitions, RF=3) and `critical-alerts` (1 partition, RF=3).
- **Lakehouse Init:** `scripts/init_lakehouse.py` connects to MinIO and creates the `raw_logs`, `processed_logs`, and `anomaly_alerts` Delta tables.
- **Database Init:** `docker/postgres/init.sql` creates the schema and 4 analytics tables (`failure_counts`, `component_health`, `rolling_error_freq`, `alert_summaries`).
- **Validation:** `scripts/health_check.py` to verify service uptime and `scripts/network_isolation_audit.py` to prove network boundaries are enforced.
- **Dependencies:** Updated `requirements.txt` with all required Spark, Kafka, ML, and FastAPI/AsyncPG libraries.

---

## 3. Commands to Execute (Bootstrap the Platform)

To launch the platform from scratch, run these commands in order from the project root:

```bash
# 1. Download datasets and generate synthetic logs (Phase 0)
bash scripts/download_datasets.sh

# 2. Launch the entire multi-network infrastructure
docker-compose up -d --build

# 3. Wait for containers to become healthy, then verify status
python scripts/health_check.py --retries 10 --delay 5

# 4. Initialize Kafka topics (requires Kafka to be up)
python scripts/setup_kafka_topics.py

# 5. Initialize the Delta Lakehouse tables on MinIO
python scripts/init_lakehouse.py

# 6. (Optional) Run the network isolation audit to verify security rules
python scripts/network_isolation_audit.py
```

---

## 4. What is Left to Build (Phase 3, 4, & 5)

The infrastructure and data ingestion are flowing into Kafka. The following components still need to be implemented:

### 🟡 Phase 3: Distributed PySpark Compute & ML Inference (`src/processing/`, `src/ml/`)
- **Schema-on-Read:** Read from the Kafka `system-logs` topic using PySpark Structured Streaming.
- **ML Pipeline:** Implement NLP Feature Engineering (Tokenizer -> HashingTF -> IDF) and Unsupervised ML (e.g., K-Means or Isolation Forest) to score anomalies.
- **Dual Egress (Bifurcation):** 
  - **Cold Path:** Write all structured logs to the Delta Lakehouse tables on MinIO and aggregate metrics to PostgreSQL via JDBC.
  - **Hot Path:** Filter `is_anomaly == 1` and write directly to the `critical-alerts` Kafka topic.

### 🟡 Phase 4: FastAPI Async Gateway & Grafana (`src/api/`)
- **FastAPI Application:** Implement the web server using `uvicorn` and FastAPI.
- **Kafka Background Consumer:** Use `aiokafka` to asynchronously consume from `critical-alerts`.
- **WebSocket Broker:** Broadcast consumed alerts in real-time to connected browser clients over WebSockets.
- **REST APIs:** Expose historical analytics endpoints reading from PostgreSQL using `asyncpg`.
- **Grafana Dashboards:** Create and provision the JSON dashboard files in `config/grafana/provisioning/dashboards/` for error trends, failure frequencies, and heatmaps.

### 🟡 Phase 5: Chaos Engineering & Fault Validation (`chaos/`)
- Write scripts to kill Fluentd, kill the Kafka controller, and terminate Spark during writes to validate the platform's resilience and end-to-end latency (< 100ms target).
