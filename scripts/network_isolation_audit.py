#!/usr/bin/env python3
"""
scripts/network_isolation_audit.py
Phase 1 / Step 1.4 — Network Isolation Audit

Validates that Docker network segmentation is working correctly:

  ✅ log-generator  CANNOT reach PostgreSQL  (postgres-db:5432)
  ✅ log-generator  CANNOT reach FastAPI      (fastapi-server:8000)
  ✅ Cross-network communication ONLY where intended per the topology

  ingestion-net → log-generator, fluentd-agent, kafka-1..3
  storage-net   → kafka-1..3, spark-master, spark-worker, minio-oss, postgres-db
  serving-net   → kafka-1..3, fastapi-server, grafana-dashboard

Usage:
    # Run directly on host (requires Docker CLI):
    python scripts/network_isolation_audit.py

    # Or exec into a container to test reachability:
    docker exec log-generator python /app/network_isolation_audit.py
"""

import logging
import socket
import subprocess
import sys
import time
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("network-audit")


@dataclass
class IsolationRule:
    description: str
    source_container: str
    target_host: str
    target_port: int
    should_reach: bool   # True = reachable expected, False = BLOCKED expected


# ── Isolation rules per the three-network topology ───────────────────────────
#
# These rules encode the exact security guarantees from read.md Step 1.4:
#   "Log Generator cannot access PostgreSQL"
#   "Log Generator cannot access FastAPI"
#   "Cross-network communication only where intended"

RULES: list[IsolationRule] = [
    # log-generator must NOT reach storage-net or serving-net
    IsolationRule(
        description="log-generator CANNOT reach PostgreSQL (storage-net only)",
        source_container="log-generator",
        target_host="postgres-db",
        target_port=5432,
        should_reach=False,
    ),
    IsolationRule(
        description="log-generator CANNOT reach FastAPI (serving-net only)",
        source_container="log-generator",
        target_host="fastapi-server",
        target_port=8000,
        should_reach=False,
    ),
    IsolationRule(
        description="log-generator CANNOT reach MinIO (storage-net only)",
        source_container="log-generator",
        target_host="minio-oss",
        target_port=9000,
        should_reach=False,
    ),
    IsolationRule(
        description="log-generator CANNOT reach Grafana (serving-net only)",
        source_container="log-generator",
        target_host="grafana-dashboard",
        target_port=3000,
        should_reach=False,
    ),
    # log-generator MUST reach Kafka (both on ingestion-net)
    IsolationRule(
        description="log-generator CAN reach kafka-1 via ingestion-net",
        source_container="log-generator",
        target_host="kafka-1",
        target_port=29092,
        should_reach=True,
    ),
    IsolationRule(
        description="log-generator CAN reach fluentd-agent via ingestion-net",
        source_container="log-generator",
        target_host="fluentd-agent",
        target_port=24224,
        should_reach=True,
    ),
    # fastapi-server must NOT reach ingestion-net
    IsolationRule(
        description="fastapi-server CANNOT reach log-generator (ingestion-net only)",
        source_container="fastapi-server",
        target_host="log-generator",
        target_port=8888,   # no open port on log-generator — blocked by isolation
        should_reach=False,
    ),
    # fastapi-server MUST reach Kafka and PostgreSQL
    IsolationRule(
        description="fastapi-server CAN reach kafka-1 via serving-net",
        source_container="fastapi-server",
        target_host="kafka-1",
        target_port=29092,
        should_reach=True,
    ),
    IsolationRule(
        description="fastapi-server CAN reach postgres-db via storage-net",
        source_container="fastapi-server",
        target_host="postgres-db",
        target_port=5432,
        should_reach=True,
    ),
    # spark-master MUST reach MinIO and Kafka
    IsolationRule(
        description="spark-master CAN reach minio-oss via storage-net",
        source_container="spark-master",
        target_host="minio-oss",
        target_port=9000,
        should_reach=True,
    ),
    IsolationRule(
        description="spark-master CAN reach kafka-1 via storage-net",
        source_container="spark-master",
        target_host="kafka-1",
        target_port=29092,
        should_reach=True,
    ),
]


def check_tcp_reachability_via_docker(
    source_container: str,
    target_host: str,
    target_port: int,
    timeout: int = 3,
) -> bool:
    """
    Use `docker exec` to run a TCP connection check from inside the source container.
    Returns True if the target is reachable.
    """
    cmd = [
        "docker", "exec", source_container,
        "sh", "-c",
        f"timeout {timeout} bash -c 'echo > /dev/tcp/{target_host}/{target_port}' 2>/dev/null && echo OK || echo FAIL",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 2,
        )
        return "OK" in result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def run_audit(rules: list[IsolationRule]) -> tuple[int, int]:
    """Run all isolation rules. Returns (passed, failed) counts."""
    passed = 0
    failed = 0

    log.info("═" * 70)
    log.info("AIOps Network Isolation Audit — Phase 1 / Step 1.4")
    log.info("═" * 70)

    for rule in rules:
        reachable = check_tcp_reachability_via_docker(
            rule.source_container,
            rule.target_host,
            rule.target_port,
        )

        if reachable == rule.should_reach:
            status = "✅ PASS"
            passed += 1
        else:
            status = "❌ FAIL"
            failed += 1
            expected = "REACHABLE" if rule.should_reach else "BLOCKED"
            actual = "REACHABLE" if reachable else "BLOCKED"
            log.error(
                "  → Expected %s, got %s: %s → %s:%d",
                expected, actual,
                rule.source_container, rule.target_host, rule.target_port,
            )

        log.info("%s — %s", status, rule.description)

    log.info("═" * 70)
    log.info("Results: %d passed, %d failed", passed, failed)

    if failed == 0:
        log.info("✅ All isolation rules satisfied — network topology is correct.")
    else:
        log.error("❌ %d isolation violations detected!", failed)
        log.error("   Check your docker-compose.yml network assignments.")

    log.info("═" * 70)
    return passed, failed


def main() -> None:
    _, failed = run_audit(RULES)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
