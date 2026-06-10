#!/usr/bin/env python3
"""
scripts/generate_logs.py
Phase 0 — Generate synthetic microservice failure logs in JSONL format.

Usage:
    python scripts/generate_logs.py --output data/raw/synthetic_logs.jsonl --count 50000
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta

# ── Constants ─────────────────────────────────────────────────────────────────

SERVICES = [
    "auth-service", "order-service", "payment-service",
    "inventory-service", "notification-service", "gateway-service",
    "user-service", "recommendation-engine", "search-service", "cart-service",
]

LOG_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "WARN", "ERROR", "ERROR", "CRITICAL"]

NORMAL_MESSAGES = [
    "Request processed successfully",
    "Cache hit for key {key}",
    "Database query executed in {ms}ms",
    "Health check passed",
    "User {user_id} authenticated",
    "Event published to topic {topic}",
    "Connection pool acquired",
    "Scheduled job completed",
    "Config refreshed from remote",
    "Metrics flushed to Prometheus",
]

ANOMALY_MESSAGES = [
    "Connection timeout after {ms}ms",
    "Circuit breaker OPEN for downstream {service}",
    "Out of memory: heap space exhausted",
    "Database connection pool exhausted",
    "Retry attempt {n}/3 failed",
    "Null pointer exception in {component}",
    "Disk I/O latency spike: {ms}ms",
    "CPU throttling detected on node {node}",
    "Kafka consumer lag exceeded threshold: {lag}",
    "SSL certificate validation failed",
    "Service {service} is UNHEALTHY — removing from load balancer",
    "Deadlock detected in transaction {txn_id}",
]

FAILURE_LABELS = {
    "Connection timeout after": "network_timeout",
    "Circuit breaker OPEN": "circuit_breaker",
    "Out of memory": "oom",
    "Database connection pool exhausted": "db_exhaustion",
    "Retry attempt": "retry_failure",
    "Null pointer exception": "npe",
    "Disk I/O latency spike": "io_spike",
    "CPU throttling": "cpu_throttle",
    "Kafka consumer lag": "kafka_lag",
    "SSL certificate": "ssl_error",
    "UNHEALTHY": "service_down",
    "Deadlock": "deadlock",
}


def _label(msg: str) -> str:
    for keyword, label in FAILURE_LABELS.items():
        if keyword in msg:
            return label
    return "normal"


def _fill(template: str) -> str:
    return template.format(
        key=f"usr:{random.randint(1000, 9999)}",
        ms=random.randint(1, 5000),
        user_id=f"U{random.randint(10000, 99999)}",
        topic=random.choice(["orders", "payments", "events", "dlq"]),
        service=random.choice(SERVICES),
        component=random.choice(["OrderController", "PaymentProcessor", "AuthFilter"]),
        node=f"node-{random.randint(1, 8)}",
        lag=random.randint(1000, 50000),
        txn_id=f"TXN{random.randint(100000, 999999)}",
        n=random.randint(1, 3),
    )


def generate_log_entry(ts: datetime, anomaly_prob: float = 0.15) -> dict:
    service = random.choice(SERVICES)
    is_anomaly = random.random() < anomaly_prob

    if is_anomaly:
        level = random.choice(["WARN", "ERROR", "CRITICAL"])
        message = _fill(random.choice(ANOMALY_MESSAGES))
    else:
        level = random.choice(LOG_LEVELS)
        message = _fill(random.choice(NORMAL_MESSAGES))

    return {
        "timestamp": ts.isoformat() + "Z",
        "service": service,
        "level": level,
        "message": message,
        "host": f"pod-{service}-{random.randint(1, 5)}",
        "trace_id": f"trace-{random.randint(100000, 999999):06x}",
        "span_id": f"span-{random.randint(10000, 99999):05x}",
        "latency_ms": random.randint(1, 5000) if is_anomaly else random.randint(1, 200),
        "is_anomaly": int(is_anomaly),
        "failure_type": _label(message),
        "env": "production",
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic microservice logs")
    parser.add_argument("--output", default="data/raw/synthetic_logs.jsonl", help="Output JSONL path")
    parser.add_argument("--count", type=int, default=50000, help="Number of log lines to generate")
    parser.add_argument("--anomaly-prob", type=float, default=0.15, help="Fraction of anomalous entries")
    args = parser.parse_args()

    start_time = datetime.utcnow() - timedelta(hours=24)
    interval = timedelta(seconds=24 * 3600 / args.count)

    print(f"Generating {args.count} log entries → {args.output}")
    written = 0
    anomalies = 0

    with open(args.output, "w") as f:
        for i in range(args.count):
            ts = start_time + interval * i
            # Inject failure bursts every ~5000 entries
            burst = (i % 5000) < 200
            prob = 0.65 if burst else args.anomaly_prob
            entry = generate_log_entry(ts, anomaly_prob=prob)
            f.write(json.dumps(entry) + "\n")
            written += 1
            anomalies += entry["is_anomaly"]

    print(f"✅ Written {written} entries | Anomalies: {anomalies} ({anomalies/written*100:.1f}%)")


if __name__ == "__main__":
    main()
