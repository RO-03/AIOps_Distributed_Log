# docker/log-generator/Dockerfile
# Phase 0 — Log Replay Engine Container
# Continuously appends BGL/synthetic dataset lines to a shared volume log file
# simulating live infrastructure log streams at 0.001s intervals.

FROM python:3.11-slim

WORKDIR /app

# No external deps needed — only stdlib
COPY log_replay.py /app/log_replay.py

# Volume mount points:
#   /var/log/aiops  — shared with fluentd-agent (write)
#   /data/raw       — dataset files (read-only)
VOLUME ["/var/log/aiops", "/data/raw"]

ENV LOG_FILE=/var/log/aiops/system.log \
    DATASET_FILE=/data/raw/BGL_2k.log \
    SLEEP_INTERVAL=0.001

CMD ["python", "/app/log_replay.py"]
