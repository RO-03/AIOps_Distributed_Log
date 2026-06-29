#!/usr/bin/env python3
"""
src/processing/stream_processor.py
Phase 3 — PySpark Structured Streaming Engine

Pipeline:
  Kafka (system-logs)
      ↓ Step 3.1 — Regex Schema-on-Read
      ↓ Step 3.2 — NLP Feature Engineering (Tokenizer → HashingTF → IDF)
      ↓ Step 3.3 — ML Inference (KMeans anomaly scoring)
      ↓ Step 3.4 — Dual Egress via foreachBatch():
            ├─ Cold: Delta Lake on MinIO (raw_logs + processed_logs)
            ├─ Hot:  Kafka critical-alerts (is_anomaly == 1)
            └─ Sync: PostgreSQL JDBC aggregation

Run inside spark-master container:
    spark-submit --master spark://spark-master:7077 \
        /app/src/processing/stream_processor.py
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import psycopg2
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("stream_processor")

# ── Configuration (env-overridable) ──────────────────────────────────────────

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
KAFKA_SRC_TOPIC = os.getenv("KAFKA_SRC_TOPIC", "system-logs")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "critical-alerts")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-oss:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "aiops_admin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "aiops_secret_2024")

LAKEHOUSE_BUCKET = "s3a://telemetry-lakehouse"
CHECKPOINT_BUCKET = "s3a://aiops-checkpoints"
MODEL_BUCKET = "s3a://aiops-models"

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://postgres-db:5432/aiops_analytics",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "aiops_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "aiops_pg_2024")

# KMeans: number of clusters. Anomalies are in the smallest / most distant cluster.
KMEANS_K = int(os.getenv("KMEANS_K", "6"))
# IDF num features for HashingTF
TF_NUM_FEATURES = int(os.getenv("TF_NUM_FEATURES", "512"))
# micro-batch trigger interval
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "10"))

# ── Kafka JSON schema (what Fluentd sends) ────────────────────────────────────

FLUENTD_SCHEMA = StructType([
    StructField("alert_code",  StringType(),  True),
    StructField("epoch",       StringType(),  True),
    StructField("date_str",    StringType(),  True),
    StructField("node_id",     StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("component",   StringType(),  True),
    StructField("severity",    StringType(),  True),
    StructField("message",     StringType(),  True),
    StructField("is_anomaly",  IntegerType(), True),
    StructField("ingested_at", StringType(),  True),
    StructField("source",      StringType(),  True),
])


# ── SparkSession factory ──────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIOps-StreamProcessor")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ── Step 3.1: Read from Kafka and parse JSON ──────────────────────────────────

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw JSON messages from the system-logs Kafka topic."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_SRC_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # Decode the Kafka value bytes to string then parse JSON
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), FLUENTD_SCHEMA).alias("d"),
        F.col("partition"),
        F.col("offset"),
    ).select(
        F.col("d.*"),
        F.col("partition"),
        F.col("offset"),
    )

    # Step 3.1: Enrich — cast types, add event_id, derived date field
    enriched = parsed.select(
        F.coalesce(F.col("alert_code"), F.lit("-")).alias("alert_code"),
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd-HH.mm.ss.SSSSSS").alias("event_time"),
        F.col("node_id").alias("host"),
        # Fix: coalesce NULL component to 'UNKNOWN' so anomaly records are always labelled
        F.coalesce(F.col("component"), F.lit("UNKNOWN")).alias("component"),
        F.col("severity"),
        F.col("message"),
        F.col("is_anomaly").cast(IntegerType()).alias("label_original"),
        F.to_timestamp(F.col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("ingested_at"),
        F.to_date(
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd-HH.mm.ss.SSSSSS")
        ).alias("log_date"),
    ).filter(F.col("message").isNotNull())

    return enriched


# ── Step 3.2: NLP Feature Engineering ────────────────────────────────────────

def build_nlp_pipeline() -> Pipeline:
    """
    Spark ML pipeline:
        message text → Tokenizer → HashingTF → IDF → feature_vector
    """
    tokenizer = Tokenizer(inputCol="message", outputCol="tokens")
    hashing_tf = HashingTF(
        inputCol="tokens",
        outputCol="raw_features",
        numFeatures=TF_NUM_FEATURES,
    )
    idf = IDF(
        inputCol="raw_features",
        outputCol="tfidf_features",
        minDocFreq=2,
    )
    assembler = VectorAssembler(
        inputCols=["tfidf_features"],
        outputCol="features",
    )
    return Pipeline(stages=[tokenizer, hashing_tf, idf, assembler])


def train_model(spark: SparkSession, sample_df: DataFrame) -> PipelineModel:
    """
    Step 3.3 — Offline training on first batch:
        NLP pipeline fit → KMeans clustering (unsupervised anomaly detection)
    """
    log.info("Training NLP + KMeans model on sample batch (%d rows)...", sample_df.count())

    nlp_pipeline = build_nlp_pipeline()
    nlp_model = nlp_pipeline.fit(sample_df)
    vectorized = nlp_model.transform(sample_df)

    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=KMEANS_K,
        seed=42,
        maxIter=20,
    )
    km_model = kmeans.fit(vectorized)

    # Save models to MinIO
    try:
        nlp_model.write().overwrite().save(f"{MODEL_BUCKET}/nlp_pipeline")
        km_model.write().overwrite().save(f"{MODEL_BUCKET}/kmeans_model")
        log.info("Models saved to %s", MODEL_BUCKET)
    except Exception as e:
        log.warning("Could not save models: %s", e)

    # Determine anomaly cluster: cluster with smallest size is most anomalous
    cluster_sizes = (
        km_model.transform(vectorized)
        .groupBy("cluster")
        .count()
        .orderBy("count")
        .collect()
    )
    anomaly_cluster = cluster_sizes[0]["cluster"]
    log.info(
        "Anomaly cluster = %d  (sizes: %s)",
        anomaly_cluster,
        [(r["cluster"], r["count"]) for r in cluster_sizes],
    )

    return nlp_model, km_model, anomaly_cluster


# ── Step 3.4: foreachBatch handler ───────────────────────────────────────────

class BatchProcessor:
    """
    Stateful foreachBatch processor.
    On first call, trains NLP + KMeans.  Subsequent calls use the cached model.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.nlp_model: Optional[PipelineModel] = None
        self.km_model = None
        self.anomaly_cluster: int = 0
        self.trained = False

    # ── Cold Path: Delta Lake ─────────────────────────────────────────────────

    def _write_delta(self, df: DataFrame, path: str, partition_cols: List[str]) -> None:
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy(*partition_cols)
            .option("mergeSchema", "true")
            .save(path)
        )

    # ── Hot Path: Kafka critical-alerts ───────────────────────────────────────

    def _publish_alerts(self, anomalies: DataFrame) -> None:
        """Serialize anomaly rows as JSON and publish to critical-alerts topic."""
        alert_df = anomalies.select(
            F.to_json(
                F.struct(
                    F.col("event_time").cast("string").alias("timestamp"),
                    F.col("component"),
                    F.col("severity"),
                    F.col("message"),
                    F.col("host"),
                    F.col("anomaly_score"),
                    F.col("cluster").alias("cluster_id"),
                )
            ).alias("value")
        )

        (
            alert_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", KAFKA_ALERT_TOPIC)
            .option("kafka.acks", "all")
            .save()
        )

    # ── Sync Path: PostgreSQL JDBC ────────────────────────────────────────────

    def _write_postgres(self, df: DataFrame) -> None:
        """Aggregate and UPSERT stats into PostgreSQL via JDBC."""
        # Aggregate by component + date for batch_metrics table
        agg = df.groupBy("component", "log_date", "severity").agg(
            F.count("*").alias("total_events"),
            F.sum(F.col("is_anomaly").cast(IntegerType())).alias("anomaly_count"),
        )

        jdbc_props = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASS,
            "driver": "org.postgresql.Driver",
        }

        try:
            (
                agg.write
                .jdbc(
                    url=POSTGRES_URL,
                    table="batch_metrics",
                    mode="append",
                    properties=jdbc_props,
                )
            )
        except Exception as e:
            log.warning("PostgreSQL JDBC write failed: %s", e)

    def _write_anomaly_alerts_pg(self, anomalies: DataFrame) -> None:
        """
        Directly persist anomaly rows into anomaly_alerts_pg via psycopg2.
        This makes the table self-contained — no dependency on FastAPI's
        Kafka consumer for DB writes. Works correctly on every Spark restart.
        """
        rows = anomalies.select(
            "event_time", "component", "severity", "message",
            "host", "anomaly_score", "cluster",
        ).collect()

        if not rows:
            return

        try:
            conn = psycopg2.connect(
                host="postgres-db",
                port=5432,
                dbname="aiops_analytics",
                user=POSTGRES_USER,
                password=POSTGRES_PASS,
                connect_timeout=10,
            )
            cur = conn.cursor()
            inserted = 0
            for row in rows:
                try:
                    cur.execute(
                        """
                        INSERT INTO anomaly_alerts_pg
                            (alert_id, event_time, component, severity, message,
                             host, anomaly_score, cluster_id)
                        VALUES (%s, %s::timestamptz, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            str(uuid.uuid4()),
                            str(row["event_time"]) if row["event_time"] else None,
                            row["component"],
                            row["severity"],
                            row["message"],
                            row["host"],
                            float(row["anomaly_score"]) if row["anomaly_score"] is not None else None,
                            int(row["cluster"]) if row["cluster"] is not None else None,
                        ),
                    )
                    inserted += 1
                except Exception as row_err:
                    log.warning("anomaly_alerts_pg row insert failed: %s", row_err)
            conn.commit()
            cur.close()
            conn.close()
            log.info("anomaly_alerts_pg: inserted %d anomaly rows", inserted)
        except Exception as e:
            log.error("anomaly_alerts_pg bulk insert failed: %s", e)

    def _update_component_health(self, df: DataFrame) -> None:
        """
        UPSERT per-component health stats into component_health.
        Keeps the Component Health Heatmap dashboard populated on every batch.
        """
        rows = df.groupBy("component").agg(
            F.count("*").alias("total_events"),
            F.sum(F.col("is_anomaly").cast(IntegerType())).alias("total_anomalies"),
            F.max("event_time").alias("last_seen"),
        ).collect()

        if not rows:
            return

        try:
            conn = psycopg2.connect(
                host="postgres-db",
                port=5432,
                dbname="aiops_analytics",
                user=POSTGRES_USER,
                password=POSTGRES_PASS,
                connect_timeout=10,
            )
            cur = conn.cursor()
            for row in rows:
                total_ev = int(row["total_events"]) or 0
                total_an = int(row["total_anomalies"]) or 0
                error_rate = round((total_an / total_ev * 100), 2) if total_ev > 0 else 0.0
                # Derive status from error_rate
                if error_rate > 10:
                    status = "critical"
                elif error_rate > 2:
                    status = "warning"
                else:
                    status = "healthy"
                cur.execute(
                    """
                    INSERT INTO component_health
                        (component, total_events, total_anomalies, error_rate_pct,
                         last_seen, status, updated_at)
                    VALUES (%s, %s, %s, %s, %s::timestamptz, %s, now())
                    ON CONFLICT (component) DO UPDATE SET
                        total_events    = component_health.total_events    + EXCLUDED.total_events,
                        total_anomalies = component_health.total_anomalies + EXCLUDED.total_anomalies,
                        error_rate_pct  = EXCLUDED.error_rate_pct,
                        last_seen       = GREATEST(component_health.last_seen, EXCLUDED.last_seen),
                        status          = EXCLUDED.status,
                        updated_at      = now()
                    """,
                    (
                        row["component"],
                        total_ev,
                        total_an,
                        error_rate,
                        str(row["last_seen"]) if row["last_seen"] else None,
                        status,
                    ),
                )
            conn.commit()
            cur.close()
            conn.close()
            log.info("component_health: upserted %d component rows", len(rows))
        except Exception as e:
            log.error("component_health upsert failed: %s", e)

    def _write_model_metrics(self, processed: DataFrame, batch_id: int) -> None:
        """
        Compute classification quality metrics by comparing KMeans predictions
        (is_anomaly) against the embedded ground-truth label (label_original).

        Metrics written to model_metrics table:
          - Accuracy  = (TP + TN) / total
          - Precision = TP / (TP + FP)   — of predicted anomalies, how many real?
          - Recall    = TP / (TP + FN)   — of real anomalies, how many caught?
          - F1        = 2 * P * R / (P + R)
        """
        # Only evaluate rows where ground truth exists
        eval_df = processed.filter(
            F.col("label_original").isNotNull()
        )
        total = eval_df.count()
        if total == 0:
            log.info("Batch %d: no labelled rows — skipping model_metrics", batch_id)
            return

        # Compute confusion matrix counts via Spark aggregation
        cm = eval_df.agg(
            F.sum(
                F.when((F.col("is_anomaly") == 1) & (F.col("label_original") == 1), 1).otherwise(0)
            ).alias("tp"),
            F.sum(
                F.when((F.col("is_anomaly") == 1) & (F.col("label_original") == 0), 1).otherwise(0)
            ).alias("fp"),
            F.sum(
                F.when((F.col("is_anomaly") == 0) & (F.col("label_original") == 0), 1).otherwise(0)
            ).alias("tn"),
            F.sum(
                F.when((F.col("is_anomaly") == 0) & (F.col("label_original") == 1), 1).otherwise(0)
            ).alias("fn"),
        ).collect()[0]

        tp = int(cm["tp"] or 0)
        fp = int(cm["fp"] or 0)
        tn = int(cm["tn"] or 0)
        fn = int(cm["fn"] or 0)

        accuracy  = (tp + tn) / total if total > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1        = (2 * precision * recall / (precision + recall)
                     if (precision + recall) > 0 else 0.0)

        log.info(
            "Batch %d — Model Metrics | total=%d | Acc=%.4f | P=%.4f | R=%.4f | F1=%.4f "
            "| TP=%d FP=%d TN=%d FN=%d",
            batch_id, total, accuracy, precision, recall, f1, tp, fp, tn, fn,
        )

        try:
            conn = psycopg2.connect(
                host="postgres-db",
                port=5432,
                dbname="aiops_analytics",
                user=POSTGRES_USER,
                password=POSTGRES_PASS,
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO model_metrics
                    (batch_id, total_count, accuracy, precision_score,
                     recall_score, f1_score,
                     true_positives, false_positives, true_negatives, false_negatives)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (batch_id, total, accuracy, precision, recall, f1,
                 tp, fp, tn, fn),
            )
            conn.commit()
            cur.close()
            conn.close()
            log.info("Batch %d: model_metrics row inserted", batch_id)
        except Exception as e:
            log.error("model_metrics DB insert failed: %s", e)

    # ── Main batch handler ────────────────────────────────────────────────────

    def process(self, batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        log.info("Batch %d: %d rows received", batch_id, count)

        if count == 0:
            log.info("Batch %d: empty, skipping", batch_id)
            return

        # Cache the batch in memory for multiple reads
        batch_df.cache()

        # ── Step 3.3: Train or reuse model ────────────────────────────────────
        if not self.trained:
            try:
                self.nlp_model, self.km_model, self.anomaly_cluster = train_model(
                    self.spark, batch_df
                )
                self.trained = True
            except Exception as e:
                log.error("Model training failed: %s", e)
                batch_df.unpersist()
                return

        # ── Apply NLP pipeline + KMeans ───────────────────────────────────────
        try:
            vectorized = self.nlp_model.transform(batch_df)
            scored = self.km_model.transform(vectorized)
        except Exception as e:
            log.error("Inference failed: %s", e)
            batch_df.unpersist()
            return

        # ── Add anomaly label + score ──────────────────────────────────────────
        anomaly_cluster = self.anomaly_cluster  # capture for lambda

        @F.udf(DoubleType())
        def dist_to_cluster_center(cluster_id, features):
            """Approximate anomaly score as normalized distance from cluster center."""
            return float(cluster_id == anomaly_cluster)

        processed = scored.select(
            F.expr("uuid()").alias("event_id"),
            F.col("event_time"),
            F.col("host"),
            F.col("component"),
            F.col("severity"),
            F.col("message"),
            F.col("log_date"),
            F.col("cluster"),
            F.when(F.col("cluster") == anomaly_cluster, 1).otherwise(0).alias("is_anomaly"),
            F.when(F.col("cluster") == anomaly_cluster, 1.0).otherwise(0.0).alias("anomaly_score"),
            # Keep ground-truth label so we can evaluate prediction quality
            F.col("label_original"),
            F.col("ingested_at"),
            F.current_timestamp().alias("processed_at"),
        )

        # ── Cold Path: write processed_logs to Delta Lake ─────────────────────
        try:
            self._write_delta(
                processed,
                f"{LAKEHOUSE_BUCKET}/processed_logs",
                ["log_date"],
            )
            log.info("Batch %d: wrote %d rows to Delta Lake", batch_id, count)
        except Exception as e:
            log.error("Delta write failed: %s", e)

        # ── Hot Path: publish anomalies to critical-alerts ────────────────────
        try:
            anomalies = processed.filter(F.col("is_anomaly") == 1)
            alert_count = anomalies.count()
            if alert_count > 0:
                self._publish_alerts(anomalies)
                log.info("Batch %d: published %d anomalies to %s", batch_id, alert_count, KAFKA_ALERT_TOPIC)
        except Exception as e:
            log.error("Kafka alert publish failed: %s", e)

        # ── Sync Path: PostgreSQL aggregation ─────────────────────────────────
        try:
            self._write_postgres(processed)
            log.info("Batch %d: PostgreSQL batch_metrics updated", batch_id)
        except Exception as e:
            log.warning("PostgreSQL batch_metrics write failed (non-fatal): %s", e)

        # ── Direct anomaly_alerts_pg insert (independent of FastAPI consumer) ──
        try:
            anomalies_for_pg = processed.filter(F.col("is_anomaly") == 1)
            self._write_anomaly_alerts_pg(anomalies_for_pg)
        except Exception as e:
            log.warning("anomaly_alerts_pg write failed (non-fatal): %s", e)

        # ── Component health UPSERT ────────────────────────────────────────────
        try:
            self._update_component_health(processed)
        except Exception as e:
            log.warning("component_health update failed (non-fatal): %s", e)

        # ── MLOps: Write classification metrics to model_metrics ───────────────
        try:
            self._write_model_metrics(processed, batch_id)
        except Exception as e:
            log.warning("model_metrics write failed (non-fatal): %s", e)

        batch_df.unpersist()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info("═" * 60)
    log.info("AIOps Stream Processor — Phase 3")
    log.info("Source topic : %s", KAFKA_SRC_TOPIC)
    log.info("Alert topic  : %s", KAFKA_ALERT_TOPIC)
    log.info("Lakehouse    : %s", LAKEHOUSE_BUCKET)
    log.info("═" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    stream_df = read_kafka_stream(spark)

    processor = BatchProcessor(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(processor.process)
        .option("checkpointLocation", f"{CHECKPOINT_BUCKET}/stream_processor")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
