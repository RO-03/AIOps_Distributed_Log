# AIOps Distributed Log Diagnostics Platform — Project Handoff

> **Last Updated:** 2026-06-10  
> **Status:** Phases 0–3 complete and verified working. Phase 4 (FastAPI full implementation + Grafana dashboards) and Phase 5 (Chaos Engineering) are next.

---

## ✅ What Is Completed & Verified Working

### Phase 0 — Dataset & Project Scaffold ✅
- BGL Supercomputer dataset (`data/raw/BGL_2k.log`) mounted and replaying
- Docker project scaffold created with all 3 networks, volumes, and services
- All Python scripts written and functional

### Phase 1 — Infrastructure ✅
- **3-broker Kafka KRaft cluster** (no ZooKeeper) — `kafka-1`, `kafka-2`, `kafka-3`
  - Valid `CLUSTER_ID: LelM2dIFQkiUFvXCEcqRWA` (22-char base64 UUID)
  - RF=3, min.insync.replicas=2, acks=all
  - Topics: `system-logs` (3 partitions), `critical-alerts` (1 partition)
- **MinIO** object storage with bucket `telemetry-lakehouse`, `aiops-checkpoints`, `aiops-models`
- **PostgreSQL 16** with 4-table analytics schema (applied manually — see commands below)
- **Spark Master + Worker** cluster (2 containers, 8 cores, 1GB RAM)
- **FastAPI** stub server (health endpoint at `:8000/health`)
- **Grafana** dashboard (`:3000`, admin/aiops_grafana)
- **3 isolated Docker bridge networks**: `aiops-ingestion-net`, `aiops-storage-net`, `aiops-serving-net`
- All services pass health checks: `python scripts/health_check.py`

### Phase 2 — Fluentd Ingestion Pipeline ✅
- **log-generator** replays BGL dataset at 0.001s interval → `/var/log/aiops/system.log`
- **Fluentd** tails log file, parses BGL regex, enriches with `is_anomaly` + `ingested_at`, forwards to Kafka `system-logs` topic
- Fix applied: `fluent.conf` uses `<format> @type json </format>` (not legacy `output_data_type`)
- Fix applied: `chunk_limit_size 1m`, `kafka_agg_max_bytes 524288` to avoid `MessageSizeTooLarge`
- Kafka `system-logs` topic actively receiving ~30K+ messages
- Verified: `docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29092 --topic system-logs --from-beginning --max-messages 5 --timeout-ms 5000`

### Phase 3 — PySpark Streaming & ML Inference ✅
- **Spark images rebuilt** with all JARs baked in:
  - `spark-sql-kafka-0-10_2.12-3.5.0.jar` (Kafka structured streaming)
  - `postgresql-42.7.1.jar` (JDBC writes)
  - `delta-spark_2.12-3.0.0.jar` + `delta-storage-3.0.0.jar`
  - `hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` (S3A/MinIO)
  - `kafka-clients-3.4.1.jar`, `commons-pool2-2.11.1.jar`, `spark-token-provider-kafka-0-10_2.12-3.5.0.jar`
- **`src/processing/stream_processor.py`** — main streaming job:
  - Step 3.1: Reads from `system-logs`, parses JSON from Fluentd
  - Step 3.2: NLP pipeline — Tokenizer → HashingTF → IDF → VectorAssembler
  - Step 3.3: KMeans (k=6) unsupervised anomaly detection (trains on first batch)
  - Step 3.4: `foreachBatch()` dual egress:
    - **Cold path**: Delta Lake → `s3a://telemetry-lakehouse/processed_logs`
    - **Hot path**: anomalies published to `critical-alerts` Kafka topic
    - **Sync path**: aggregated metrics to PostgreSQL via JDBC
- **`src/ml/train_model.py`** — standalone offline model retraining script
- **`scripts/submit_spark_job.py`** — helper to `docker cp` + `spark-submit`
- **PostgreSQL schema** applied: `batch_metrics`, `anomaly_alerts_pg`, `component_health`, `alert_summary`
- Delta Lake tables initialized: `raw_logs`, `processed_logs`, `anomaly_alerts` (confirmed version=0)
- Streaming job connects to Spark cluster (`app-20260610145930-0000`), executor on `spark-worker` with 8 cores

---

## ❌ What Is NOT Yet Done

| Phase | Task | Status |
|---|---|---|
| Phase 4 | Full FastAPI async gateway (WebSockets, Kafka consumer, REST APIs) | ❌ Only stub exists |
| Phase 4 | Grafana dashboard JSON provisioning | ❌ Not configured |
| Phase 5 | Chaos engineering validation suite | ❌ Not started |

---

## Known Issues Resolved (Do Not Re-Introduce)

| Issue | Fix |
|---|---|
| `kafka-python` broken on Python 3.11+ | Use `kafka-python-ng==2.2.3` |
| `pyarrow==12.0.1` fails on Python 3.12 host | Use `pyarrow==14.0.2`, `pandas==2.1.4`, `numpy==1.26.4` locally |
| `pyarrow/pandas` inside Spark Docker (Python 3.8) | Use `pyarrow==12.0.1`, `pandas==2.0.3`, `numpy==1.24.4` in Dockerfile |
| `<format> section is required` in Fluentd | Use `<format> @type json </format>` block, not `output_data_type` |
| `Kafka::MessageSizeTooLarge` in Fluentd | `chunk_limit_size 1m`, `kafka_agg_max_bytes 524288`; Kafka `MESSAGE_MAX_BYTES: 2097152` |
| Kafka `CLUSTER_ID` invalid | Must be exactly 22-char base64 UUID: `LelM2dIFQkiUFvXCEcqRWA` |
| Kafka `kafka-init` bash syntax error | Multi-line commands must use single-line or correct backslash escaping |
| FastAPI crash loop | Needs `src/api/main.py` with valid FastAPI app and `/health` endpoint |
| Grafana datasource crash | Only ONE datasource can have `isDefault: true` |
| `python scripts/init_lakehouse.py` fails | Needs local Java. Run via Docker: `docker exec spark-master python3 /tmp/init_lakehouse.py` |
| `list[str]` type hints in Python 3.8 Spark container | Use `from typing import List, Optional` instead |
| PostgreSQL `init.sql` not applied on existing volume | Apply manually in cmd: `type docker\postgres\init.sql | docker exec -i postgres-db psql -U aiops_user -d aiops_analytics` |
| Delta schema/partition mismatch (`stream_processor.py`) | `init_lakehouse.py` updated to match Spark output exactly, partitioned by `log_date` |
| PostgreSQL `BatchUpdateException` (`stream_processor.py`) | Used `F.to_date` instead of `F.date_format` so `log_date` is a correct `DATE` type |
| `UnboundLocalError` in `setup_kafka_topics.py` | Initialized `all_ok = True` variable before execution |
| Spark `submit_spark_job.py` Ivy cache download fails | Replaced `--packages` with `--jars` to use pre-baked local JARs inside container |
| `train_model.py` "Column already exists" error | Dropped ML-generated columns (`cluster`, etc.) before fitting KMeans pipeline |

---

## Project File Map

```
aiops/
├── docker-compose.yml              ← All 11 services, 3 networks, 8 volumes
├── requirements.txt                ← Python deps (host, Python 3.12)
├── read.md                         ← Project spec / requirements
│
├── config/
│   ├── fluentd/fluent.conf         ← Fluentd parsing pipeline (BGL regex → Kafka)
│   └── grafana/provisioning/
│       └── datasources/            ← Grafana datasource YAML (prometheus.yml only default)
│
├── data/raw/BGL_2k.log             ← BGL Supercomputer dataset (2K rows)
│
├── docker/
│   ├── spark/
│   │   ├── Dockerfile              ← Spark image with ALL JARs baked in (Phase 3)
│   │   └── spark-defaults.conf     ← S3A/MinIO + Delta Lake + Kafka config
│   ├── fluentd/Dockerfile          ← Fluentd with fluent-plugin-kafka, build-base, ruby-dev
│   ├── fastapi/Dockerfile          ← FastAPI server image
│   ├── log-generator/Dockerfile    ← BGL replay engine
│   └── postgres/init.sql           ← 4-table PostgreSQL analytics schema
│
├── scripts/
│   ├── health_check.py             ← TCP + HTTP health check for all 11 services
│   ├── setup_kafka_topics.py       ← Creates system-logs + critical-alerts topics
│   ├── init_lakehouse.py           ← Creates Delta Lake tables on MinIO (run in Docker)
│   └── submit_spark_job.py         ← Copies src/ + spark-submit inside container
│
└── src/
    ├── api/main.py                 ← FastAPI stub (health endpoint only)
    ├── processing/
    │   └── stream_processor.py     ← Phase 3 main streaming job (Kafka→ML→Delta+Kafka+PG)
    └── ml/
        └── train_model.py          ← Standalone offline KMeans model trainer
```

---

## Service URLs (All Running)

| Service | URL | Credentials |
|---|---|---|
| FastAPI | http://localhost:8000/health | — |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | aiops_admin / aiops_secret_2024 |
| Grafana | http://localhost:3000 | admin / aiops_grafana |
| PostgreSQL | localhost:5432 | aiops_user / aiops_pg_2024 / aiops_analytics |
| Kafka Broker 1 | localhost:9092 | — |
| Kafka Broker 2 | localhost:9094 | — |
| Kafka Broker 3 | localhost:9096 | — |

---

## Commands — Complete Reference

### 🔴 Stop Everything
```powershell
# Stop containers, keep data volumes
docker-compose stop

# Full teardown (deletes ALL data/volumes — clean slate)
docker-compose down -v
```

---

### 🟢 Start From Scratch (Fresh Boot)

```powershell
# 1. Build all images (first time or after Dockerfile changes)
docker-compose build

# 2. Start infrastructure in background
docker-compose up -d

# 3. Wait ~60s for Kafka KRaft election, then verify all healthy
python scripts/health_check.py --retries 10 --delay 10

# 4. Apply PostgreSQL schema (REQUIRED on first run or after volume reset)
# Note: Use `Get-Content` if in PowerShell, or `type` if in standard Command Prompt (cmd.exe)
type docker\postgres\init.sql | docker exec -i postgres-db psql -U aiops_user -d aiops_analytics

# 5. Verify Kafka topics exist (created by kafka-init container automatically)
python scripts/setup_kafka_topics.py

# 6. Initialize Delta Lake tables on MinIO (run INSIDE spark-master)
docker cp scripts/init_lakehouse.py spark-master:/tmp/init_lakehouse.py
docker exec spark-master python3 /tmp/init_lakehouse.py
```

---

### 🟢 Start From Existing State (Containers Already Built)

```powershell
# Start all containers
docker-compose up -d

# Verify health
python scripts/health_check.py

# Check Fluentd is streaming to Kafka (should show JSON log records)
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29092 --topic system-logs --from-beginning --max-messages 5 --timeout-ms 5000
```

---

### 🔵 Phase 3 — Run the Spark Streaming Job

```powershell
# Option A: Use the helper script (copies src/ + submits)
python scripts/submit_spark_job.py --job stream_processor

# Option B: Manual (copy then submit)
docker cp src/. spark-master:/app/src
docker exec spark-master spark-submit `
  --master spark://spark-master:7077 `
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar,/opt/spark/jars/postgresql-42.7.1.jar,/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /app/src/processing/stream_processor.py
```

---

### 🔵 Phase 3 — Run Offline Model Trainer

```powershell
docker cp src/. spark-master:/app/src
docker exec spark-master spark-submit `
  --master spark://spark-master:7077 `
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/postgresql-42.7.1.jar,/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /app/src/ml/train_model.py --source delta
```

---

### 🔍 Verification Commands

```powershell
# Check all container statuses
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check Kafka topic offsets (growing = Fluentd is healthy)
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic system-logs --time -1

# Verify critical-alerts topic has anomalies (after streaming job runs)
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29092 --topic critical-alerts --from-beginning --max-messages 5 --timeout-ms 8000

# Check PostgreSQL batch_metrics (populated by Spark JDBC)
docker exec postgres-db psql -U aiops_user -d aiops_analytics -c "SELECT component, log_date, total_events, anomaly_count FROM batch_metrics LIMIT 10;"

# Check MinIO Delta Lake tables via Spark
docker exec spark-master python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.hadoop.fs.s3a.endpoint','http://minio-oss:9000').config('spark.hadoop.fs.s3a.access.key','aiops_admin').config('spark.hadoop.fs.s3a.secret.key','aiops_secret_2024').config('spark.hadoop.fs.s3a.path.style.access','true').config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate()
spark.read.format('delta').load('s3a://telemetry-lakehouse/processed_logs').show(5, truncate=False)
"

# If Fluentd gets MessageSizeTooLarge (emergency fix):
docker exec fluentd-agent rm -rf /fluentd/buffer/kafka_output
docker restart fluentd-agent
```

---

### 🔧 Rebuild After Code Changes

```powershell
# Rebuild only Spark images (e.g., after Dockerfile changes)
docker-compose build spark-master spark-worker
docker-compose up -d spark-master spark-worker

# Rebuild only Fluentd
docker-compose build fluentd-agent
docker restart fluentd-agent

# Rebuild only FastAPI
docker-compose build fastapi-server
docker-compose up -d fastapi-server

# Push updated Python scripts to Spark container (no rebuild needed)
docker cp src/. spark-master:/app/src
```

---

## Phase 4 — What Needs to Be Built Next

The existing `src/api/main.py` is a **stub only** (just `/health`). The full implementation needs:

1. **`src/api/main.py`** (full replacement):
   - FastAPI lifespan startup → launches `aiokafka` background consumer on `critical-alerts`
   - WebSocket endpoint: `GET /ws/v1/live-alerts` — broadcasts anomaly events to browser clients
   - REST endpoints: `GET /api/v1/metrics/summary`, `GET /api/v1/alerts/recent`
   - Async PostgreSQL queries via `asyncpg`
   - Prometheus metrics endpoint

2. **Grafana dashboard JSON** (`config/grafana/provisioning/dashboards/`):
   - Error trend charts from `batch_metrics`
   - Anomaly timeline from `anomaly_alerts_pg`
   - Component health heatmap from `component_health`

3. **WebSocket client** (optional HTML page for demo)

---

## Environment — Key Facts for Any LLM

| Fact | Value |
|---|---|
| Python in Spark containers | **Python 3.8** (apache/spark:3.5.0 base) — use `List`, `Optional` from `typing` |
| Python on host Windows | **Python 3.12** (`.venv`) |
| Kafka internal listener | `kafka-1:29092`, `kafka-2:29092`, `kafka-3:29092` (inside Docker) |
| Kafka external listener | `localhost:9092`, `localhost:9094`, `localhost:9096` (host machine) |
| MinIO S3 endpoint | `http://minio-oss:9000` (inside Docker) / `http://localhost:9000` (host) |
| PostgreSQL JDBC URL | `jdbc:postgresql://postgres-db:5432/aiops_analytics` (inside Docker) |
| Spark master URL | `spark://spark-master:7077` (inside Docker) |
| Kafka CLUSTER_ID | `LelM2dIFQkiUFvXCEcqRWA` (must be exactly 22-char base64 UUID) |
| init.sql | Does NOT auto-apply on existing volume — must pipe manually via `Get-Content` |
| `docker cp src/.` | Copies `src/` into spark-master at `/app/src` — no rebuild needed for script changes |
| Shell | PowerShell — use backtick `` ` `` not `\` for line continuation; use `Get-Content \|` not `<` for piping |
