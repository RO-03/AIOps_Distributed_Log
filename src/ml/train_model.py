#!/usr/bin/env python3
"""
src/ml/train_model.py
Phase 3 / Step 3.3 — Offline Model Training Script

Trains a NLP + KMeans anomaly detection model on historical data from Delta Lake
(or on a provided CSV file) and saves the model artifacts to MinIO.

This script can be run independently for model retraining without restarting
the streaming job.

Usage (inside spark-master container):
    spark-submit --master spark://spark-master:7077 \
        /app/src/ml/train_model.py [--source delta|csv] [--input <path>]
"""

import argparse
import logging
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler, Normalizer
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("train_model")

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio-oss:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "aiops_admin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "aiops_secret_2024")

LAKEHOUSE_BUCKET = "s3a://telemetry-lakehouse"
MODEL_BUCKET     = "s3a://aiops-models"

KMEANS_K         = int(os.getenv("KMEANS_K", "6"))
TF_NUM_FEATURES  = int(os.getenv("TF_NUM_FEATURES", "512"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIOps-ModelTrainer")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def load_training_data(spark: SparkSession, source: str, input_path: str):
    """Load training data from Delta Lake or CSV."""
    if source == "delta":
        path = input_path or f"{LAKEHOUSE_BUCKET}/processed_logs"
        log.info("Loading training data from Delta Lake: %s", path)
        df = spark.read.format("delta").load(path)
    else:
        log.info("Loading training data from CSV: %s", input_path)
        df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Ensure message column exists and is not null
    df = df.filter(F.col("message").isNotNull() & (F.length(F.col("message")) > 0))
    
    # Drop columns that the ML pipeline will create to avoid "Column already exists" errors
    # (These columns exist if we are reading from processed_logs)
    columns_to_drop = ["cluster", "tokens", "raw_features", "tfidf_features", "features", "anomaly_score"]
    df = df.drop(*columns_to_drop)
    count = df.count()
    log.info("Loaded %d training rows", count)
    if count == 0:
        log.error("No data available for training. Exiting.")
        sys.exit(1)
    return df


def train(spark: SparkSession, df):
    """Build and fit the NLP + KMeans pipeline."""

    log.info("Step 3.2 — Building NLP feature pipeline...")

    tokenizer  = Tokenizer(inputCol="message", outputCol="tokens")
    hashing_tf = HashingTF(
        inputCol="tokens",
        outputCol="raw_features",
        numFeatures=TF_NUM_FEATURES,
    )
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features", minDocFreq=2)
    assembler = VectorAssembler(inputCols=["tfidf_features"], outputCol="raw_assembled")
    normalizer = Normalizer(inputCol="raw_assembled", outputCol="features", p=2.0)

    nlp_pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf, assembler, normalizer])

    log.info("Fitting NLP pipeline...")
    nlp_model = nlp_pipeline.fit(df)
    vectorized = nlp_model.transform(df)

    log.info("Step 3.3 — Training KMeans (k=%d)...", KMEANS_K)
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=KMEANS_K,
        seed=42,
        maxIter=20,
    )
    km_model = kmeans.fit(vectorized)

    # Evaluate silhouette score
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    predictions = km_model.transform(vectorized)
    silhouette = evaluator.evaluate(predictions)
    log.info("KMeans silhouette score: %.4f", silhouette)

    # Determine anomaly cluster: cluster with highest density of ground-truth anomalies
    if "is_anomaly" in df.columns:
        anomaly_cluster_row = (
            predictions.groupBy("cluster")
            .agg(F.avg("is_anomaly").alias("anomaly_density"))
            .orderBy(F.desc("anomaly_density"))
            .collect()
        )
        anomaly_cluster = anomaly_cluster_row[0]["cluster"]
        log.info(
            "Anomaly cluster determined by ground-truth density: %d (density: %.4f)",
            anomaly_cluster, anomaly_cluster_row[0]["anomaly_density"]
        )
    else:
        # Fallback to smallest cluster
        cluster_sizes = (
            predictions.groupBy("cluster")
            .count()
            .orderBy("count")
            .collect()
        )
        anomaly_cluster = cluster_sizes[0]["cluster"]
        log.info(
            "Anomaly cluster = %d  (all cluster sizes: %s)",
            anomaly_cluster,
            {r["cluster"]: r["count"] for r in cluster_sizes},
        )

    # ── Offline classification evaluation (when ground truth is available) ───
    if "is_anomaly" in [f.name for f in df.schema.fields]:
        log.info("Evaluating model predictions against ground-truth 'is_anomaly' labels...")
        eval_df = predictions.withColumn(
            "predicted",
            (predictions["cluster"] == anomaly_cluster).cast("int"),
        ).filter(predictions["is_anomaly"].isNotNull())

        total = eval_df.count()
        if total > 0:
            from pyspark.sql.functions import when, sum as _sum, col
            cm_row = eval_df.agg(
                _sum(when((col("predicted") == 1) & (col("is_anomaly") == 1), 1).otherwise(0)).alias("tp"),
                _sum(when((col("predicted") == 1) & (col("is_anomaly") == 0), 1).otherwise(0)).alias("fp"),
                _sum(when((col("predicted") == 0) & (col("is_anomaly") == 0), 1).otherwise(0)).alias("tn"),
                _sum(when((col("predicted") == 0) & (col("is_anomaly") == 1), 1).otherwise(0)).alias("fn"),
            ).collect()[0]
            tp, fp, tn, fn = (
                int(cm_row["tp"] or 0), int(cm_row["fp"] or 0),
                int(cm_row["tn"] or 0), int(cm_row["fn"] or 0),
            )
            accuracy  = (tp + tn) / total
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1        = (2 * precision * recall / (precision + recall)
                         if (precision + recall) > 0 else 0.0)
            log.info("=" * 55)
            log.info("  OFFLINE MODEL EVALUATION REPORT")
            log.info("  Total evaluated samples : %d", total)
            log.info("  Accuracy                : %.4f  (%.2f%%)", accuracy, accuracy * 100)
            log.info("  Precision               : %.4f", precision)
            log.info("  Recall                  : %.4f", recall)
            log.info("  F1-Score                : %.4f", f1)
            log.info("  Confusion Matrix:")
            log.info("    True Positives  (TP)  : %d", tp)
            log.info("    False Positives (FP)  : %d", fp)
            log.info("    True Negatives  (TN)  : %d", tn)
            log.info("    False Negatives (FN)  : %d", fn)
            log.info("  KMeans Silhouette Score : %.4f", silhouette)
            log.info("=" * 55)
        else:
            log.warning("No rows with ground-truth labels found for evaluation.")
    else:
        log.info("No 'is_anomaly' column in training data — skipping evaluation.")

    return nlp_model, km_model, anomaly_cluster


def save_models(nlp_model, km_model, anomaly_cluster: int):
    """Persist trained models to MinIO."""
    log.info("Saving NLP pipeline to %s/nlp_pipeline ...", MODEL_BUCKET)
    nlp_model.write().overwrite().save(f"{MODEL_BUCKET}/nlp_pipeline")

    log.info("Saving KMeans model to %s/kmeans_model ...", MODEL_BUCKET)
    km_model.write().overwrite().save(f"{MODEL_BUCKET}/kmeans_model")

    # Save metadata as a tiny text file for the streaming job to read
    import boto3
    import json

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-oss:9000",
        aws_access_key_id="aiops_admin",
        aws_secret_access_key="aiops_secret_2024",
    )
    metadata = {
        "anomaly_cluster": anomaly_cluster,
        "kmeans_k": KMEANS_K,
        "tf_num_features": TF_NUM_FEATURES,
    }
    s3.put_object(
        Bucket="aiops-models",
        Key="model_metadata.json",
        Body=json.dumps(metadata).encode(),
    )
    log.info("Model metadata saved: %s", metadata)


def main():
    parser = argparse.ArgumentParser(description="Train AIOps anomaly detection model")
    parser.add_argument(
        "--source", choices=["delta", "csv"], default="delta",
        help="Data source for training (default: delta)",
    )
    parser.add_argument(
        "--input", default=None,
        help="Override input path (default: telemetry-lakehouse/processed_logs)",
    )
    args = parser.parse_args()

    log.info("═" * 60)
    log.info("AIOps Anomaly Detection — Offline Model Training")
    log.info("Source : %s", args.source)
    log.info("═" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = load_training_data(spark, args.source, args.input)
    nlp_model, km_model, anomaly_cluster = train(spark, df)
    save_models(nlp_model, km_model, anomaly_cluster)

    log.info("═" * 60)
    log.info("✅ Model training complete.")
    log.info("   NLP pipeline → %s/nlp_pipeline", MODEL_BUCKET)
    log.info("   KMeans model → %s/kmeans_model", MODEL_BUCKET)
    log.info("   Anomaly cluster ID = %d", anomaly_cluster)
    log.info("═" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
