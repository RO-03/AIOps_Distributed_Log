#!/usr/bin/env python3
"""
scripts/submit_spark_job.py
Phase 3 — Helper script to spark-submit the streaming job inside the container.

Copies the src/ tree into the spark-master container and runs spark-submit.

Usage (from project root, with Docker running):
    python scripts/submit_spark_job.py [--job stream_processor|train_model]
"""

import argparse
import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("submit_spark_job")

CONTAINER = "spark-master"
APP_DIR   = "/app"

# Kafka Spark SQL connector (must match spark 3.5 + scala 2.12)
KAFKA_JAR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
POSTGRES_JAR = "org.postgresql:postgresql:42.7.1"

JOBS = {
    "stream_processor": {
        "script": f"{APP_DIR}/src/processing/stream_processor.py",
        "description": "Phase 3 — PySpark Structured Streaming + ML Inference",
    },
    "train_model": {
        "script": f"{APP_DIR}/src/ml/train_model.py",
        "description": "Phase 3 — Offline KMeans Model Trainer",
    },
}


def copy_src_to_container() -> None:
    """Sync src/ folder into the container."""
    log.info("Copying src/ to %s:%s ...", CONTAINER, APP_DIR)
    result = subprocess.run(
        ["docker", "cp", "src/.", f"{CONTAINER}:{APP_DIR}/src"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error("docker cp failed: %s", result.stderr)
        sys.exit(1)
    log.info("Source files copied successfully.")


def submit(job_name: str, extra_args: list[str]) -> None:
    job = JOBS[job_name]
    log.info("Submitting: %s", job["description"])

    cmd = [
        "docker", "exec", CONTAINER,
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", f"{KAFKA_JAR},{POSTGRES_JAR}",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", f"spark.hadoop.fs.s3a.endpoint=http://minio-oss:9000",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.access.key=aiops_admin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=aiops_secret_2024",
        "--conf", "spark.sql.shuffle.partitions=8",
        job["script"],
    ] + extra_args

    log.info("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd, text=True)
    sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Submit a PySpark job to the Docker Spark cluster")
    parser.add_argument(
        "--job", choices=list(JOBS.keys()), default="stream_processor",
        help="Which job to submit (default: stream_processor)",
    )
    parser.add_argument(
        "extra", nargs=argparse.REMAINDER,
        help="Extra arguments forwarded to the PySpark script",
    )
    args = parser.parse_args()

    copy_src_to_container()
    submit(args.job, args.extra)


if __name__ == "__main__":
    main()
