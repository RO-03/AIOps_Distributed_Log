#!/usr/bin/env python3
"""
docker/log-generator/log_replay.py
Phase 0 / Step 2.2 — Continuous Append Engine

Behavior:
    Read Dataset Line
          ↓
    Append To Active Log File
          ↓
    Sleep 0.001 Seconds
          ↓
    Repeat (loop back to start of dataset when exhausted)

Environment variables:
    LOG_FILE        — path to the file Fluentd tails (default: /var/log/aiops/system.log)
    DATASET_FILE    — source dataset to replay (default: /data/raw/BGL_2k.log)
    SLEEP_INTERVAL  — seconds between appends (default: 0.001)

Purpose:
    Simulate live infrastructure logs for the Fluentd → Kafka ingestion pipeline.
"""

import os
import sys
import time
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] log-generator — %(message)s",
)
log = logging.getLogger("log-generator")

LOG_FILE = os.environ.get("LOG_FILE", "/var/log/aiops/system.log")
DATASET_FILE = os.environ.get("DATASET_FILE", "/data/raw/BGL_2k.log")
SLEEP_INTERVAL = float(os.environ.get("SLEEP_INTERVAL", "0.001"))

# Lines to write per flush to avoid excessive syscall overhead
BATCH_SIZE = 10


def ensure_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def load_dataset(path: str) -> list[str]:
    """Load all lines from the dataset file, stripping blanks."""
    dataset_path = Path(path)
    if not dataset_path.exists():
        log.error("Dataset file not found: %s", path)
        log.error("Mount the data/raw directory and ensure BGL_2k.log exists.")
        sys.exit(1)

    with open(dataset_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]

    log.info("Loaded %d log lines from %s", len(lines), path)
    return lines


def replay(lines: list[str], log_file: str, sleep_interval: float) -> None:
    """Continuously append dataset lines to the target log file."""
    pass_count = 0
    total_written = 0

    while True:
        pass_count += 1
        log.info("Pass %d — replaying %d lines to %s", pass_count, len(lines), log_file)

        with open(log_file, "a", encoding="utf-8", buffering=1) as fh:
            batch = []
            for i, line in enumerate(lines):
                batch.append(line)
                if len(batch) >= BATCH_SIZE:
                    fh.write("\n".join(batch) + "\n")
                    fh.flush()
                    total_written += len(batch)
                    batch = []
                    time.sleep(sleep_interval * BATCH_SIZE)

            # flush remaining
            if batch:
                fh.write("\n".join(batch) + "\n")
                total_written += len(batch)

        log.info(
            "Pass %d complete — total lines written: %d",
            pass_count,
            total_written,
        )


def main() -> None:
    log.info("═" * 60)
    log.info("AIOps Log Replay Engine — Phase 0")
    log.info("Dataset   : %s", DATASET_FILE)
    log.info("Target    : %s", LOG_FILE)
    log.info("Interval  : %.4fs per line (batch=%d)", SLEEP_INTERVAL, BATCH_SIZE)
    log.info("═" * 60)

    ensure_dir(LOG_FILE)
    lines = load_dataset(DATASET_FILE)
    replay(lines, LOG_FILE, SLEEP_INTERVAL)


if __name__ == "__main__":
    main()
