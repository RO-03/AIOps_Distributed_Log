#!/usr/bin/env python3
"""
scripts/init_lakehouse.py
Phase 1 — Initialize Delta Lake schema on MinIO.

Creates three Delta tables:
  - raw_logs       : Ingested log events (append-only)
  - processed_logs : Feature-engineered log records
  - anomaly_alerts : ML inference results + failure predictions

Run AFTER `docker-compose up -d` and minio-init completes.

Usage:
    python scripts/init_lakehouse.py [--master spark://localhost:7077]
"""

import argparse
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, TimestampType, BooleanType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("init_lakehouse")

# ── Schema definitions ────────────────────────────────────────────────────────

RAW_LOGS_SCHEMA = StructType([
    StructField("event_id",      StringType(),    nullable=False),
    StructField("timestamp",     TimestampType(), nullable=False),
    StructField("service",       StringType(),    nullable=True),
    StructField("level",         StringType(),    nullable=True),
    StructField("message",       StringType(),    nullable=True),
    StructField("host",          StringType(),    nullable=True),
    StructField("trace_id",      StringType(),    nullable=True),
    StructField("span_id",       StringType(),    nullable=True),
    StructField("latency_ms",    IntegerType(),   nullable=True),
    StructField("is_anomaly",    IntegerType(),   nullable=True),
    StructField("failure_type",  StringType(),    nullable=True),
    StructField("env",           StringType(),    nullable=True),
    StructField("ingested_at",   TimestampType(), nullable=True),
])

PROCESSED_LOGS_SCHEMA = StructType([
    StructField("event_id",            StringType(),  nullable=False),
    StructField("timestamp",           TimestampType(), nullable=False),
    StructField("service",             StringType(),  nullable=True),
    StructField("level_encoded",       IntegerType(), nullable=True),
    StructField("message_len",         IntegerType(), nullable=True),
    StructField("latency_ms",          IntegerType(), nullable=True),
    StructField("latency_zscore",      DoubleType(),  nullable=True),
    StructField("is_error",            BooleanType(), nullable=True),
    StructField("hour_of_day",         IntegerType(), nullable=True),
    StructField("day_of_week",         IntegerType(), nullable=True),
    StructField("rolling_error_rate",  DoubleType(),  nullable=True),
    StructField("failure_type",        StringType(),  nullable=True),
    StructField("is_anomaly",          IntegerType(), nullable=True),
    StructField("processed_at",        TimestampType(), nullable=True),
])

ANOMALY_ALERTS_SCHEMA = StructType([
    StructField("alert_id",           StringType(),    nullable=False),
    StructField("event_id",           StringType(),    nullable=True),
    StructField("timestamp",          TimestampType(), nullable=False),
    StructField("service",            StringType(),    nullable=True),
    StructField("anomaly_score",      DoubleType(),    nullable=True),
    StructField("predicted_label",    StringType(),    nullable=True),
    StructField("confidence",         DoubleType(),    nullable=True),
    StructField("model_version",      StringType(),    nullable=True),
    StructField("alert_severity",     StringType(),    nullable=True),
    StructField("triggered_at",       TimestampType(), nullable=True),
    StructField("acknowledged",       BooleanType(),   nullable=True),
])

# Spec Step 1.3: MinIO bucket name = telemetry-lakehouse
TABLES = {
    "raw_logs":       ("s3a://telemetry-lakehouse/raw_logs",       RAW_LOGS_SCHEMA,       "timestamp"),
    "processed_logs": ("s3a://telemetry-lakehouse/processed_logs", PROCESSED_LOGS_SCHEMA, "timestamp"),
    "anomaly_alerts": ("s3a://telemetry-lakehouse/anomaly_alerts", ANOMALY_ALERTS_SCHEMA, "triggered_at"),
}


# ── Spark session factory ─────────────────────────────────────────────────────

def build_spark(master: str) -> SparkSession:
    log.info("Building SparkSession (master=%s)", master)
    return (
        SparkSession.builder
        .master(master)
        .appName("AIOps-LakehouseInit")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-oss:9000")
        .config("spark.hadoop.fs.s3a.access.key", "aiops_admin")
        .config("spark.hadoop.fs.s3a.secret.key", "aiops_secret_2024")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ── Table creation ─────────────────────────────────────────────────────────────

def create_table(spark: SparkSession, name: str, path: str, schema: StructType, partition_col: str):
    """Create a Delta table at the given S3A path if it doesn't already exist."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        log.info("Table '%s' already exists at %s — skipping", name, path)
        return

    log.info("Creating Delta table '%s' at %s (partitioned by %s)", name, path, partition_col)
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )
    log.info("✅ Table '%s' created", name)


def verify_table(spark: SparkSession, name: str, path: str):
    """Read table metadata to verify the table is accessible."""
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, path)
    history = dt.history(1)
    log.info(
        "Verified '%s': version=%s, operation=%s",
        name,
        history.first()["version"],
        history.first()["operation"],
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Initialize AIOps Delta Lake schema")
    parser.add_argument("--master", default="spark://spark-master:7077", help="Spark master URL (container: spark-master:7077)")
    args = parser.parse_args()

    try:
        spark = build_spark(args.master)
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        if "JAVA_GATEWAY_EXITED" in str(e) or "Java gateway" in str(e):
            log.error("Java is missing locally! Run this script via Docker instead:")
            log.error("    docker exec spark-master python /app/scripts/init_lakehouse.py")
            sys.exit(1)
        raise

    log.info("═" * 60)
    log.info("AIOps Lakehouse Initialization — Phase 1")
    log.info("═" * 60)

    errors = []
    for table_name, (path, schema, partition_col) in TABLES.items():
        try:
            create_table(spark, table_name, path, schema, partition_col)
            verify_table(spark, table_name, path)
        except Exception as exc:
            log.error("Failed to initialize table '%s': %s", table_name, exc)
            errors.append(table_name)

    spark.stop()

    if errors:
        log.error("Initialization failed for tables: %s", errors)
        sys.exit(1)

    log.info("═" * 60)
    log.info("✅ All Delta tables initialized successfully.")
    log.info("   Bucket        → s3a://telemetry-lakehouse/")
    log.info("   MinIO console → http://localhost:9001")
    log.info("   Spark UI       → http://localhost:8081")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
