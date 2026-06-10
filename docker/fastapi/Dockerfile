# docker/fastapi/Dockerfile
# Phase 4 — FastAPI Async Gateway Container
# Provides WebSocket real-time alert delivery and REST historical analytics.

FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Application source
COPY . /app

EXPOSE 8000

# Entrypoint — run uvicorn with auto-reload disabled in production
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
