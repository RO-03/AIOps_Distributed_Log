# docs/setup_guide.md — Phase 0 & Phase 1 Run Guide

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin) — v24+
- Python 3.10+
- Git
- 8 GB RAM available for Docker (Spark + Kafka + MinIO)

---

## Phase 0 — GitHub & Local Setup

### 1. Init local repo and push

```bash
cd path/to/AIOps_Distributed_Log   # this project root

git init
git add .
git commit -m "feat: Phase 0 — project scaffold, directory structure, dataset scripts"
git branch -M main
git remote add origin https://github.com/RO-03/AIOps_Distributed_Log.git
git push -u origin main
```

### 2. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Download datasets

```bash
bash scripts/download_datasets.sh
```

This downloads:
- `data/raw/HDFS_2k.log`  — HDFS log sample from Loghub
- `data/raw/BGL_2k.log`   — BGL supercomputer log sample
- `data/raw/synthetic_logs.jsonl` — 50k generated microservice logs

---

## Phase 1 — Infrastructure Bring-Up

### 1. Build and start all containers

```bash
docker-compose up -d --build
```

Expected startup order (allow ~2 min for full health):
```
zookeeper → kafka → minio → minio-init → spark-master → spark-workers → prometheus → grafana
```

### 2. Verify all services are healthy

```bash
python scripts/health_check.py --retries 10 --delay 8
```

### 3. Create Kafka topics

```bash
python scripts/setup_kafka_topics.py
```

### 4. Initialize Delta Lake schema on MinIO

```bash
python scripts/init_lakehouse.py
```

---

## Service Endpoints

| Service         | URL                          | Credentials           |
|-----------------|------------------------------|-----------------------|
| Kafka UI        | http://localhost:8080        | —                     |
| MinIO Console   | http://localhost:9001        | aiops_admin / aiops_secret_2024 |
| Spark Master UI | http://localhost:8081        | —                     |
| Prometheus      | http://localhost:9090        | —                     |
| Grafana         | http://localhost:3000        | admin / aiops_grafana |

---

## Verify Delta Tables on MinIO

1. Open MinIO Console at http://localhost:9001
2. Login with `aiops_admin` / `aiops_secret_2024`
3. Navigate to bucket `aiops-lakehouse`
4. You should see three folders: `raw_logs/`, `processed_logs/`, `anomaly_alerts/`
5. Each folder contains a `_delta_log/` subdirectory — confirms Delta table creation

---

## Tear Down

```bash
docker-compose down -v    # -v removes named volumes (resets MinIO data)
```
