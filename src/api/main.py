# src/api/main.py
# Phase 4 — FastAPI Async Gateway
# Step 4.1: aiokafka background consumer on 'critical-alerts'
# Step 4.2: WebSocket /ws/v1/live-alerts  — broadcasts to all connected clients
# Step 4.3: REST   GET /api/v1/metrics/summary
#                  GET /api/v1/alerts/recent
#                  GET /api/v1/health/components
# Step 4.4: Prometheus metrics via /metrics
#
# Env vars (set in docker-compose.yml):
#   KAFKA_BOOTSTRAP_SERVERS  kafka-1:29092,kafka-2:29092,kafka-3:29092
#   KAFKA_ALERT_TOPIC        critical-alerts
#   POSTGRES_DSN             postgresql://aiops_user:aiops_pg_2024@postgres-db:5432/aiops_analytics

import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import PlainTextResponse, HTMLResponse
from loguru import logger
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Environment ───────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
)
KAFKA_ALERT_TOPIC: str = os.getenv("KAFKA_ALERT_TOPIC", "critical-alerts")
POSTGRES_DSN: str = os.getenv(
    "POSTGRES_DSN",
    "postgresql://aiops_user:aiops_pg_2024@postgres-db:5432/aiops_analytics",
)

# ── Prometheus Metrics ────────────────────────────────────────────────────────

ALERTS_RECEIVED_TOTAL = Counter(
    "aiops_alerts_received_total",
    "Total anomaly alerts consumed from Kafka critical-alerts topic",
)
WS_CLIENTS_ACTIVE = Gauge(
    "aiops_websocket_clients_active",
    "Number of currently connected WebSocket clients",
)
WS_MESSAGES_SENT_TOTAL = Counter(
    "aiops_websocket_messages_sent_total",
    "Total messages broadcast to WebSocket clients",
)
API_REQUEST_DURATION = Histogram(
    "aiops_api_request_duration_seconds",
    "FastAPI endpoint latency",
    ["endpoint"],
)
KAFKA_CONSUMER_LAG = Gauge(
    "aiops_kafka_consumer_lag",
    "Estimated Kafka consumer lag for critical-alerts topic",
)

# ── Pydantic Models ───────────────────────────────────────────────────────────

class AlertEvent(BaseModel):
    timestamp: Optional[str] = None
    component: Optional[str] = None
    severity: Optional[str] = None
    message: Optional[str] = None
    host: Optional[str] = None
    anomaly_score: Optional[float] = None
    cluster_id: Optional[int] = None
    received_at: str = ""


class MetricsSummary(BaseModel):
    component: str
    log_date: str
    total_events: int
    anomaly_count: int
    anomaly_rate_pct: float


class ComponentHealth(BaseModel):
    component: str
    total_events: int
    total_anomalies: int
    error_rate_pct: Optional[float]
    last_seen: Optional[str]
    status: str


# ── WebSocket Connection Manager ──────────────────────────────────────────────

class ConnectionManager:
    """Thread-safe manager for active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.append(ws)
        WS_CLIENTS_ACTIVE.set(len(self._connections))
        logger.info(f"WS client connected. Total={len(self._connections)}")

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._connections = [c for c in self._connections if c is not ws]
        WS_CLIENTS_ACTIVE.set(len(self._connections))
        logger.info(f"WS client disconnected. Total={len(self._connections)}")

    async def broadcast(self, payload: str) -> None:
        async with self._lock:
            live = list(self._connections)
        dead: List[WebSocket] = []
        for ws in live:
            try:
                await ws.send_text(payload)
                WS_MESSAGES_SENT_TOTAL.inc()
            except Exception:
                dead.append(ws)
        # clean up dead connections
        if dead:
            async with self._lock:
                self._connections = [c for c in self._connections if c not in dead]
            WS_CLIENTS_ACTIVE.set(len(self._connections))


manager = ConnectionManager()

# ── Shared App State ──────────────────────────────────────────────────────────

class AppState:
    db_pool: Optional[asyncpg.Pool] = None
    kafka_consumer: Optional[AIOKafkaConsumer] = None
    kafka_task: Optional[asyncio.Task] = None


state = AppState()

# ── Kafka Consumer Loop ───────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30))
async def _create_kafka_consumer() -> AIOKafkaConsumer:
    """Create and start an AIOKafkaConsumer, retrying on failure."""
    consumer = AIOKafkaConsumer(
        KAFKA_ALERT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fastapi-alert-gateway-v4",
        auto_offset_reset="earliest",  # fetch historical alerts too
        enable_auto_commit=True,
        value_deserializer=lambda b: b.decode("utf-8", errors="replace"),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
    )
    await consumer.start()
    logger.info(
        f"Kafka consumer started. topic={KAFKA_ALERT_TOPIC} "
        f"servers={KAFKA_BOOTSTRAP_SERVERS}"
    )
    return consumer


async def kafka_consumer_loop() -> None:
    """Persist background task: consume critical-alerts and broadcast via WS."""
    while True:
        try:
            state.kafka_consumer = await _create_kafka_consumer()
            async for msg in state.kafka_consumer:
                try:
                    raw = msg.value
                    # Try to parse JSON; fall back to raw string
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        data = {"raw": raw}

                    alert = AlertEvent(
                        timestamp=data.get("timestamp") or data.get("event_time"),
                        component=data.get("component"),
                        severity=data.get("severity"),
                        message=data.get("message"),
                        host=data.get("host"),
                        anomaly_score=data.get("anomaly_score"),
                        cluster_id=data.get("cluster_id"),
                        received_at=datetime.utcnow().isoformat() + "Z",
                    )
                    ALERTS_RECEIVED_TOTAL.inc()
                    
                    # Insert into PostgreSQL anomaly_alerts_pg
                    if state.db_pool:
                        try:
                            alert_id = str(uuid.uuid4())
                            async with state.db_pool.acquire() as conn:
                                await conn.execute(
                                    """
                                    INSERT INTO anomaly_alerts_pg (alert_id, event_time, component, severity, message, host, anomaly_score, cluster_id)
                                    VALUES ($1, $2::timestamp, $3, $4, $5, $6, $7, $8)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    alert_id, alert.timestamp, alert.component, alert.severity, alert.message, alert.host, float(alert.anomaly_score) if alert.anomaly_score is not None else None, int(alert.cluster_id) if alert.cluster_id is not None else None
                                )
                        except Exception as e_db:
                            logger.error(f"Error inserting alert to db: {e_db}")

                    payload = alert.model_dump_json()
                    await manager.broadcast(payload)
                    logger.debug(
                        f"Alert broadcast: component={alert.component} severity={alert.severity}"
                    )
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
        except asyncio.CancelledError:
            logger.info("Kafka consumer task cancelled — shutting down.")
            break
        except Exception as e:
            logger.error(f"Kafka consumer loop crashed: {e}. Restarting in 5s…")
            await asyncio.sleep(5)
        finally:
            if state.kafka_consumer:
                try:
                    await state.kafka_consumer.stop()
                except Exception:
                    pass
                state.kafka_consumer = None


# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("FastAPI startup: initialising PostgreSQL connection pool…")
    try:
        state.db_pool = await asyncpg.create_pool(
            dsn=POSTGRES_DSN,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        logger.info("PostgreSQL pool ready.")
    except Exception as e:
        logger.error(f"PostgreSQL pool failed: {e}. REST queries will be unavailable.")
        state.db_pool = None

    logger.info("FastAPI startup: launching Kafka consumer background task…")
    state.kafka_task = asyncio.create_task(kafka_consumer_loop())

    yield  # ── Application is running ─────────────────────────────────────

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("FastAPI shutdown: stopping Kafka consumer…")
    if state.kafka_task:
        state.kafka_task.cancel()
        try:
            await state.kafka_task
        except asyncio.CancelledError:
            pass

    if state.kafka_consumer:
        await state.kafka_consumer.stop()

    if state.db_pool:
        await state.db_pool.close()
        logger.info("PostgreSQL pool closed.")

    logger.info("FastAPI shutdown complete.")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="AIOps Distributed Log Diagnostics — API Gateway",
    description=(
        "Real-time anomaly alert WebSocket feed and historical analytics REST API "
        "for the AIOps Lakehouse platform."
    ),
    version="4.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Health Endpoint ───────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"], summary="Service health check")
async def health_check():
    """Returns service liveness. Used by Docker health-check probe."""
    db_ok = state.db_pool is not None
    kafka_ok = state.kafka_consumer is not None
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dependencies": {
            "postgres": "connected" if db_ok else "degraded",
            "kafka_consumer": "running" if kafka_ok else "starting",
        },
    }


# ── Prometheus Metrics Endpoint ───────────────────────────────────────────────

@app.get("/metrics", tags=["ops"], response_class=PlainTextResponse,
         summary="Prometheus scrape endpoint")
async def prometheus_metrics():
    """Exposes Prometheus metrics for scraping."""
    return PlainTextResponse(
        generate_latest().decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )


# ── WebSocket — Live Alert Feed ───────────────────────────────────────────────

@app.websocket("/ws/v1/live-alerts")
async def websocket_live_alerts(websocket: WebSocket):
    """
    WebSocket endpoint — streams real-time anomaly alerts consumed from the
    Kafka 'critical-alerts' topic. Each message is a JSON-serialised AlertEvent.

    Connect with:  ws://localhost:8000/ws/v1/live-alerts
    """
    await manager.connect(websocket)
    try:
        # Send a welcome handshake so the client knows the stream is live
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": "AIOps live alert stream active. Waiting for anomalies…",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }))
        # Keep the connection alive — messages arrive via broadcast()
        while True:
            # Receive any client heartbeat / ping to detect disconnection
            await asyncio.wait_for(websocket.receive_text(), timeout=30)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(websocket)


# ── REST — Metrics Summary ────────────────────────────────────────────────────

@app.get(
    "/api/v1/metrics/summary",
    response_model=List[MetricsSummary],
    tags=["analytics"],
    summary="Error trends from batch_metrics table",
)
async def get_metrics_summary(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    component: Optional[str] = Query(None, description="Filter by component name"),
):
    """
    Returns aggregated error trends per (component, date) from the PostgreSQL
    `batch_metrics` table written by PySpark JDBC.
    """
    t0 = time.monotonic()
    if not state.db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    query = """
        SELECT
            component,
            log_date::text AS log_date,
            SUM(total_events)  AS total_events,
            SUM(anomaly_count) AS anomaly_count,
            CASE
                WHEN SUM(total_events) > 0
                THEN ROUND(SUM(anomaly_count)::numeric / SUM(total_events) * 100, 2)
                ELSE 0
            END AS anomaly_rate_pct
        FROM batch_metrics
    """
    params: list = []
    if component:
        query += " WHERE component = $1"
        params.append(component)
    query += " GROUP BY component, log_date ORDER BY log_date DESC, anomaly_count DESC"

    async with state.db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    API_REQUEST_DURATION.labels(endpoint="/api/v1/metrics/summary").observe(
        time.monotonic() - t0
    )
    return [
        MetricsSummary(
            component=r["component"],
            log_date=r["log_date"],
            total_events=r["total_events"],
            anomaly_count=r["anomaly_count"],
            anomaly_rate_pct=float(r["anomaly_rate_pct"]),
        )
        for r in rows
    ]


# ── REST — Recent Alerts ──────────────────────────────────────────────────────

@app.get(
    "/api/v1/alerts/recent",
    tags=["analytics"],
    summary="Recent anomaly alerts from anomaly_alerts_pg",
)
async def get_recent_alerts(
    limit: int = Query(50, ge=1, le=500, description="Maximum number of alerts to return"),
    component: Optional[str] = Query(None, description="Filter by component name"),
    severity: Optional[str] = Query(None, description="Filter by severity (e.g. FATAL, ERROR)"),
):
    """
    Returns the most recent anomaly alert events from PostgreSQL `anomaly_alerts_pg`
    written by the PySpark streaming job.
    """
    t0 = time.monotonic()
    if not state.db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    conditions = []
    params: list = []
    if component:
        params.append(component)
        conditions.append(f"component = ${len(params)}")
    if severity:
        params.append(severity.upper())
        conditions.append(f"severity = ${len(params)}")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)
    query = f"""
        SELECT
            alert_id,
            event_time::text AS event_time,
            component,
            severity,
            message,
            host,
            anomaly_score,
            cluster_id,
            created_at::text AS created_at
        FROM anomaly_alerts_pg
        {where_clause}
        ORDER BY event_time DESC NULLS LAST
        LIMIT ${len(params)}
    """

    async with state.db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    API_REQUEST_DURATION.labels(endpoint="/api/v1/alerts/recent").observe(
        time.monotonic() - t0
    )
    return [dict(r) for r in rows]


# ── REST — Component Health ───────────────────────────────────────────────────

@app.get(
    "/api/v1/health/components",
    response_model=List[ComponentHealth],
    tags=["analytics"],
    summary="Component health heatmap data from component_health",
)
async def get_component_health():
    """
    Returns the rolling health status for each BGL component from the PostgreSQL
    `component_health` table (maintained by PySpark streaming job via JDBC upserts).
    """
    t0 = time.monotonic()
    if not state.db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    query = """
        SELECT
            component,
            total_events,
            total_anomalies,
            error_rate_pct,
            last_seen::text AS last_seen,
            status
        FROM component_health
        ORDER BY total_anomalies DESC
    """
    async with state.db_pool.acquire() as conn:
        rows = await conn.fetch(query)

    API_REQUEST_DURATION.labels(endpoint="/api/v1/health/components").observe(
        time.monotonic() - t0
    )
    return [
        ComponentHealth(
            component=r["component"],
            total_events=r["total_events"],
            total_anomalies=r["total_anomalies"],
            error_rate_pct=r["error_rate_pct"],
            last_seen=r["last_seen"],
            status=r["status"],
        )
        for r in rows
    ]


# ── REST — Alert Summary (daily roll-up) ──────────────────────────────────────

@app.get(
    "/api/v1/alerts/summary",
    tags=["analytics"],
    summary="Daily alert summary from alert_summary table",
)
async def get_alert_summary(
    days: int = Query(7, ge=1, le=90),
):
    """Daily alert frequency roll-up per component from `alert_summary`."""
    t0 = time.monotonic()
    if not state.db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    query = """
        SELECT
            summary_date::text,
            component,
            alert_count,
            event_count,
            anomaly_rate
        FROM alert_summary
        ORDER BY summary_date DESC, alert_count DESC
    """
    async with state.db_pool.acquire() as conn:
        rows = await conn.fetch(query)

    API_REQUEST_DURATION.labels(endpoint="/api/v1/alerts/summary").observe(
        time.monotonic() - t0
    )
    return [dict(r) for r in rows]


# ── Demo WebSocket Client (optional HTML page) ────────────────────────────────

DEMO_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>AIOps — Live Alert Monitor</title>
  <style>
    :root {
      --bg: #0d1117; --surface: #161b22; --border: #30363d;
      --accent: #f85149; --green: #3fb950; --yellow: #d29922;
      --text: #e6edf3; --muted: #8b949e; --font: 'Fira Mono', monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: var(--font); min-height: 100vh; }
    header {
      background: var(--surface); border-bottom: 1px solid var(--border);
      padding: 16px 24px; display: flex; align-items: center; gap: 12px;
    }
    .logo { width: 32px; height: 32px; background: var(--accent);
             border-radius: 6px; display:grid; place-items:center; font-size: 18px; }
    h1 { font-size: 18px; font-weight: 600; }
    .badge {
      margin-left: auto; padding: 4px 10px; border-radius: 20px;
      font-size: 12px; font-weight: 600; letter-spacing: 0.05em;
      background: #21262d; border: 1px solid var(--border);
    }
    .badge.live { background: rgba(63,185,80,0.15); border-color: var(--green); color: var(--green); }
    .badge.offline { background: rgba(248,81,73,0.15); border-color: var(--accent); color: var(--accent); }
    main { padding: 24px; max-width: 1100px; margin: 0 auto; }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px; }
    .stat-card {
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 8px; padding: 20px;
    }
    .stat-card .label { font-size: 12px; color: var(--muted); text-transform: uppercase;
                         letter-spacing: 0.08em; margin-bottom: 8px; }
    .stat-card .value { font-size: 28px; font-weight: 700; }
    .stat-card .value.red { color: var(--accent); }
    .stat-card .value.green { color: var(--green); }
    #feed-container {
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 8px; overflow: hidden;
    }
    .feed-header {
      padding: 12px 16px; border-bottom: 1px solid var(--border);
      font-size: 13px; color: var(--muted); display: flex; justify-content: space-between;
    }
    #feed {
      height: 520px; overflow-y: auto; padding: 8px;
      display: flex; flex-direction: column-reverse;
    }
    .alert-row {
      padding: 10px 12px; border-radius: 6px; margin-bottom: 4px;
      border-left: 3px solid transparent; font-size: 13px;
      animation: slideIn 0.2s ease;
    }
    @keyframes slideIn { from { opacity:0; transform:translateY(-6px); } to { opacity:1; } }
    .alert-row.fatal  { border-color: var(--accent); background: rgba(248,81,73,0.08); }
    .alert-row.error  { border-color: var(--yellow); background: rgba(210,153,34,0.08); }
    .alert-row.warn   { border-color: #58a6ff; background: rgba(88,166,255,0.08); }
    .alert-row.info   { border-color: var(--border); background: rgba(48,54,61,0.3); }
    .alert-meta { display: flex; gap: 10px; margin-bottom: 4px; flex-wrap: wrap; }
    .alert-meta .ts  { color: var(--muted); }
    .alert-meta .cmp { color: #58a6ff; font-weight: 600; }
    .alert-meta .sev { padding: 1px 6px; border-radius: 4px; font-size: 11px; font-weight: 700; }
    .sev-FATAL { background: var(--accent); color: #fff; }
    .sev-ERROR { background: var(--yellow); color: #000; }
    .sev-WARN  { background: #58a6ff; color: #000; }
    .msg { color: var(--text); font-size: 12px; word-break: break-all; }
    .system-msg { color: var(--muted); font-style: italic; padding: 8px 12px; }
    button#clear-btn {
      background: none; border: 1px solid var(--border); color: var(--muted);
      padding: 4px 10px; border-radius: 4px; font-family: var(--font);
      font-size: 12px; cursor: pointer;
    }
    button#clear-btn:hover { border-color: var(--accent); color: var(--accent); }
  </style>
</head>
<body>
  <header>
    <div class="logo">⚡</div>
    <h1>AIOps — Live Alert Monitor</h1>
    <span class="badge offline" id="conn-badge">⬤ Disconnected</span>
  </header>
  <main>
    <div class="stats">
      <div class="stat-card">
        <div class="label">Alerts Received</div>
        <div class="value red" id="stat-total">0</div>
      </div>
      <div class="stat-card">
        <div class="label">FATAL / ERROR</div>
        <div class="value red" id="stat-fatal">0</div>
      </div>
      <div class="stat-card">
        <div class="label">Connected Since</div>
        <div class="value green" id="stat-since">—</div>
      </div>
    </div>
    <div id="feed-container">
      <div class="feed-header">
        <span>📡 Real-time Anomaly Feed — <code>/ws/v1/live-alerts</code></span>
        <button id="clear-btn" onclick="clearFeed()">Clear</button>
      </div>
      <div id="feed">
        <div class="system-msg">Connecting to alert stream…</div>
      </div>
    </div>
  </main>
  <script>
    let total = 0, fatal = 0;
    const feed = document.getElementById('feed');
    const badge = document.getElementById('conn-badge');

    function clearFeed() { feed.innerHTML = ''; }

    function appendRow(html) {
      const wrapper = document.createElement('div');
      wrapper.innerHTML = html;
      feed.prepend(wrapper.firstElementChild);
      // keep max 200 entries
      while (feed.children.length > 200) feed.removeChild(feed.lastChild);
    }

    function connect() {
      const ws = new WebSocket(`ws://${location.host}/ws/v1/live-alerts`);

      ws.onopen = () => {
        badge.className = 'badge live';
        badge.textContent = '⬤ Live';
        const since = new Date().toLocaleTimeString();
        document.getElementById('stat-since').textContent = since;
        appendRow('<div class="system-msg">✅ Stream connected.</div>');
        // heartbeat
        setInterval(() => { if (ws.readyState === 1) ws.send('ping'); }, 20000);
      };

      ws.onmessage = (ev) => {
        const data = JSON.parse(ev.data);
        if (data.type === 'connected') {
          appendRow(`<div class="system-msg">${data.message}</div>`);
          return;
        }
        total++;
        document.getElementById('stat-total').textContent = total;
        const sev = (data.severity || 'INFO').toUpperCase();
        if (sev === 'FATAL' || sev === 'ERROR') {
          fatal++;
          document.getElementById('stat-fatal').textContent = fatal;
        }
        const cls = sev === 'FATAL' ? 'fatal' : sev === 'ERROR' ? 'error' :
                    sev === 'WARN' ? 'warn' : 'info';
        const ts = data.timestamp || data.received_at || '';
        const msg = (data.message || '').substring(0, 200);
        appendRow(`
          <div class="alert-row ${cls}">
            <div class="alert-meta">
              <span class="ts">${ts}</span>
              <span class="cmp">${data.component || '?'}</span>
              <span class="sev sev-${sev}">${sev}</span>
            </div>
            <div class="msg">${msg}</div>
          </div>`);
      };

      ws.onerror = () => {
        badge.className = 'badge offline';
        badge.textContent = '⬤ Error';
      };

      ws.onclose = () => {
        badge.className = 'badge offline';
        badge.textContent = '⬤ Reconnecting…';
        appendRow('<div class="system-msg">⚠ Stream disconnected. Reconnecting in 5s…</div>');
        setTimeout(connect, 5000);
      };
    }

    connect();
  </script>
</body>
</html>
"""


@app.get("/demo", tags=["ops"], response_class=HTMLResponse, summary="Live alert demo UI")
async def demo_page():
    """Browser-based live anomaly alert monitor (WebSocket demo client)."""
    return HTMLResponse(content=DEMO_HTML)
