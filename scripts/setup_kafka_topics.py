#!/usr/bin/env python3
"""
scripts/setup_kafka_topics.py
Phase 1 / Step 2.4 — Create Kafka topics for the AIOps platform.

Topics (per spec):
    system-logs     — Partitions: 3, Replication Factor: 3, retention 24h
                      Purpose: Bulk log ingestion
    critical-alerts — Partitions: 1, Replication Factor: 3, retention 7 days
                      Purpose: Low-latency alert delivery

Producer requirement: acks=all (required_acks=-1)

Run after all 3 Kafka brokers are healthy:
    python scripts/setup_kafka_topics.py [--bootstrap kafka-1:29092]
"""

import argparse
import logging
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("kafka_setup")

# ── Topic definitions (spec: Step 2.4) ───────────────────────────────────────
#
#  name              partitions  replication_factor  retention_ms
TOPICS = [
    (
        "system-logs",          # bulk log ingestion topic
        3,                      # 3 partitions for parallelism
        3,                      # RF=3, all 3 brokers
        86_400_000,             # 24h retention
    ),
    (
        "critical-alerts",      # low-latency alert delivery topic
        1,                      # 1 partition, ordered delivery
        3,                      # RF=3 for durability
        604_800_000,            # 7 days retention
    ),
]

# Producer config that MUST be set on producers writing to these topics
PRODUCER_REQUIREMENTS = {
    "acks": "all",              # acks=all — wait for all in-sync replicas
    "enable.idempotence": True, # exactly-once semantics
    "min.insync.replicas": 2,   # topic-level min ISR
}


def wait_for_broker(bootstrap: str, max_wait: int = 60) -> None:
    """Poll until at least one broker is reachable."""
    log.info("Waiting for Kafka broker at %s (max %ds)...", bootstrap, max_wait)
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            client = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="aiops-healthcheck",
                request_timeout_ms=5000,
            )
            client.close()
            log.info("✅ Broker reachable: %s", bootstrap)
            return
        except NoBrokersAvailable:
            time.sleep(3)
    log.error("Broker not reachable after %ds. Aborting.", max_wait)
    sys.exit(1)


def create_topics(bootstrap: str) -> bool:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="aiops-topic-setup",
    )

    new_topics = []
    for name, partitions, replication, retention_ms in TOPICS:
        new_topics.append(
            NewTopic(
                name=name,
                num_partitions=partitions,
                replication_factor=replication,
                topic_configs={
                    "retention.ms": str(retention_ms),
                    # min.insync.replicas must match acks=all guarantee
                    "min.insync.replicas": "2",
                    # compact cleanup for alerts, delete for raw logs
                    "cleanup.policy": "delete",
                },
            )
        )

    results = admin.create_topics(new_topics=new_topics, validate_only=False)
    all_ok = True
    for topic, future in results.items():
        try:
            future.result()
            log.info("✅ Created topic: %s", topic)
        except TopicAlreadyExistsError:
            log.info("   Topic already exists (skipped): %s", topic)
        except Exception as exc:
            log.error("❌ Failed to create topic %s: %s", topic, exc)
            all_ok = False

    # Verify topics
    existing = admin.list_topics()
    for name, *_ in TOPICS:
        if name in existing:
            log.info("✔  Verified topic exists: %s", name)
        else:
            log.error("✘  Topic NOT found after creation: %s", name)
            all_ok = False

    admin.close()
    return all_ok


def print_producer_requirements() -> None:
    log.info("─" * 50)
    log.info("Producer requirements for all topics:")
    for k, v in PRODUCER_REQUIREMENTS.items():
        log.info("  %-30s = %s", k, v)
    log.info("─" * 50)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create AIOps Kafka topics")
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap server (default: localhost:9092 — use kafka-1:29092 inside Docker)",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=60,
        help="Max seconds to wait for broker (default: 60)",
    )
    args = parser.parse_args()

    log.info("═" * 60)
    log.info("AIOps Kafka Topic Setup — Phase 1 / Step 2.4")
    log.info("Bootstrap: %s", args.bootstrap)
    log.info("═" * 60)

    wait_for_broker(args.bootstrap, args.wait)
    ok = create_topics(args.bootstrap)
    print_producer_requirements()

    if ok:
        log.info("═" * 60)
        log.info("✅ Kafka topic setup complete.")
        log.info("   Topics: %s", [t[0] for t in TOPICS])
        log.info("═" * 60)
        sys.exit(0)
    else:
        log.error("Topic setup had errors. Check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
