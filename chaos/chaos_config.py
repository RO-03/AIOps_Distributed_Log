#!/usr/bin/env python3
"""
chaos/chaos_config.py
Phase 5 — Shared configuration constants for all chaos scenarios.
Import from any scenario to get consistent values.
"""

# ── Docker container names ─────────────────────────────────────────────────────
CONTAINER_KAFKA_1 = "kafka-1"
CONTAINER_KAFKA_2 = "kafka-2"
CONTAINER_KAFKA_3 = "kafka-3"
CONTAINER_FLUENTD = "fluentd-agent"
CONTAINER_SPARK_MASTER = "spark-master"
CONTAINER_SPARK_WORKER = "spark-worker"
CONTAINER_MINIO = "minio-oss"
CONTAINER_POSTGRES = "postgres-db"
CONTAINER_FASTAPI = "fastapi-server"
CONTAINER_GRAFANA = "grafana-dashboard"
CONTAINER_LOG_GENERATOR = "log-generator"

# ── Kafka ──────────────────────────────────────────────────────────────────────
# Internal (inside Docker networks)
KAFKA_INTERNAL_BOOTSTRAP = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
KAFKA_INTERNAL_1 = "kafka-1:29092"
# External (host machine)
KAFKA_EXTERNAL_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"
KAFKA_EXTERNAL_1 = "localhost:9092"

KAFKA_TOPIC_LOGS = "system-logs"
KAFKA_TOPIC_ALERTS = "critical-alerts"

# ── MinIO ──────────────────────────────────────────────────────────────────────
MINIO_INTERNAL_ENDPOINT = "http://minio-oss:9000"
MINIO_EXTERNAL_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "aiops_admin"
MINIO_SECRET_KEY = "aiops_secret_2024"
MINIO_BUCKET_LAKEHOUSE = "telemetry-lakehouse"
MINIO_BUCKET_CHECKPOINTS = "aiops-checkpoints"
MINIO_BUCKET_MODELS = "aiops-models"

# ── Delta Lake ─────────────────────────────────────────────────────────────────
DELTA_PATH_RAW = "s3a://telemetry-lakehouse/raw_logs"
DELTA_PATH_PROCESSED = "s3a://telemetry-lakehouse/processed_logs"
DELTA_PATH_ANOMALIES = "s3a://telemetry-lakehouse/anomaly_alerts"

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
POSTGRES_INTERNAL_DSN = (
    "postgresql://aiops_user:aiops_pg_2024@postgres-db:5432/aiops_analytics"
)
POSTGRES_EXTERNAL_DSN = (
    "postgresql://aiops_user:aiops_pg_2024@localhost:5432/aiops_analytics"
)

# ── FastAPI ────────────────────────────────────────────────────────────────────
FASTAPI_BASE_URL = "http://localhost:8000"
FASTAPI_WS_URL = "ws://localhost:8000/ws/v1/live-alerts"
FASTAPI_HEALTH_URL = "http://localhost:8000/health"
FASTAPI_ALERTS_URL = "http://localhost:8000/api/v1/alerts/recent?limit=5"
FASTAPI_METRICS_URL = "http://localhost:8000/api/v1/metrics/summary"

# ── Grafana ────────────────────────────────────────────────────────────────────
GRAFANA_URL = "http://localhost:3000"
GRAFANA_HEALTH_URL = "http://localhost:3000/api/health"

# ── Spark ──────────────────────────────────────────────────────────────────────
SPARK_MASTER_URL = "spark://spark-master:7077"
SPARK_UI_URL = "http://localhost:8081"
SPARK_JARS = (
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
    "/opt/spark/jars/kafka-clients-3.4.1.jar,"
    "/opt/spark/jars/commons-pool2-2.11.1.jar,"
    "/opt/spark/jars/postgresql-42.7.1.jar,"
    "/opt/spark/jars/delta-spark_2.12-3.0.0.jar,"
    "/opt/spark/jars/delta-storage-3.0.0.jar,"
    "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
)

# ── Latency targets ────────────────────────────────────────────────────────────
TARGET_FLUENTD_KAFKA_MS = 10_000    # Fluentd buffering can add latency
TARGET_KAFKA_FASTAPI_WS_MS = 5_000  # Kafka→WebSocket should be fast
TARGET_E2E_MS = 15_000              # Total end-to-end for Docker env

# ── Chaos timing ──────────────────────────────────────────────────────────────
FLUENTD_OUTAGE_SECONDS = 20
FLUENTD_RECOVERY_WAIT_SECONDS = 45
KAFKA_RE_ELECTION_TIMEOUT = 60
KAFKA_REJOIN_TIMEOUT = 60
SPARK_KILL_DELAY_SECONDS = 15
SPARK_RESTART_WAIT_SECONDS = 30
KAFKA_SCAN_TIMEOUT = 90
WEBSOCKET_WAIT_TIMEOUT = 30
