#!/usr/bin/env python3
"""
scripts/setup_kafka_topics.py
Phase 1 — Create all required Kafka topics for the AIOps platform.

Run after Kafka is healthy:
    python scripts/setup_kafka_topics.py
"""

import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka_setup")

BOOTSTRAP = "localhost:9092"

TOPICS = [
    # name,                  partitions, replication, retention_ms
    ("aiops.raw-logs",           3,          1,        86_400_000),   # 24h
    ("aiops.processed-logs",     3,          1,        86_400_000),
    ("aiops.anomaly-alerts",     1,          1,        604_800_000),  # 7 days
    ("aiops.dlq",                1,          1,        604_800_000),  # dead-letter queue
]


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="aiops-setup")
    new_topics = []
    for name, partitions, replication, retention_ms in TOPICS:
        new_topics.append(NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replication,
            topic_configs={"retention.ms": str(retention_ms)},
        ))

    results = admin.create_topics(new_topics=new_topics, validate_only=False)
    for topic, future in results.items():
        try:
            future.result()
            log.info("✅ Created topic: %s", topic)
        except TopicAlreadyExistsError:
            log.info("   Topic already exists (skipped): %s", topic)
        except Exception as exc:
            log.error("❌ Failed to create topic %s: %s", topic, exc)

    admin.close()
    log.info("Kafka topic setup complete.")


if __name__ == "__main__":
    create_topics()
