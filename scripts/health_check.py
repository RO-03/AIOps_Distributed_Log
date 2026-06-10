#!/usr/bin/env python3
"""
scripts/health_check.py
Phase 1 — Verify all AIOps infrastructure services are reachable and healthy.

Usage:
    python scripts/health_check.py
    python scripts/health_check.py --retries 10 --delay 5
"""

import argparse
import logging
import sys
import time
import urllib.request
from urllib.error import URLError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("health_check")

SERVICES = [
    ("Kafka UI",      "http://localhost:8080"),
    ("MinIO API",     "http://localhost:9000/minio/health/live"),
    ("MinIO Console", "http://localhost:9001"),
    ("Spark Master",  "http://localhost:8081"),
    ("Spark Worker1", "http://localhost:8082"),
    ("Spark Worker2", "http://localhost:8083"),
    ("Prometheus",    "http://localhost:9090/-/healthy"),
    ("Grafana",       "http://localhost:3000/api/health"),
]


def check(name: str, url: str, timeout: int = 5) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            ok = resp.status < 400
            status = "✅" if ok else "❌"
            log.info("%s %-20s → HTTP %s", status, name, resp.status)
            return ok
    except URLError as e:
        log.warning("❌ %-20s → UNREACHABLE (%s)", name, e.reason)
        return False
    except Exception as e:
        log.warning("❌ %-20s → ERROR (%s)", name, e)
        return False


def run_checks(retries: int, delay: int):
    for attempt in range(1, retries + 1):
        log.info("── Health Check Attempt %d/%d ──────────────────", attempt, retries)
        results = {name: check(name, url) for name, url in SERVICES}
        failed = [name for name, ok in results.items() if not ok]

        if not failed:
            log.info("══ All services healthy ✅ ══")
            return True

        log.warning("Still waiting for: %s", failed)
        if attempt < retries:
            log.info("Retrying in %ds...\n", delay)
            time.sleep(delay)

    log.error("Health check failed after %d attempts. Check docker-compose logs.", retries)
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--retries", type=int, default=5, help="Number of retry attempts")
    parser.add_argument("--delay",   type=int, default=10, help="Seconds between retries")
    args = parser.parse_args()

    ok = run_checks(args.retries, args.delay)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
