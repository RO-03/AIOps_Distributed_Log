-- docker/postgres/init.sql
-- Phase 1 / Step 1.3 — PostgreSQL Analytics Tables
--
-- Tables required by the spec:
--   ✅ failure_counts       — Failure counts per component/service
--   ✅ component_health     — Component health scores (rolling)
--   ✅ rolling_error_freq   — Rolling error frequencies per time window
--   ✅ alert_summaries      — Alert event log / summary per anomaly
--
-- This file is executed automatically by the PostgreSQL container on first start.

-- ── Schema ────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS aiops;

-- ── Table 1: failure_counts ───────────────────────────────────────────────────
-- Stores aggregated failure counts per component, severity, and time window.
-- Written by the PySpark JDBC sync path.

CREATE TABLE IF NOT EXISTS aiops.failure_counts (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    component       VARCHAR(255) NOT NULL,
    severity        VARCHAR(50)  NOT NULL,     -- INFO, WARN, ERROR, CRITICAL, FATAL
    failure_type    VARCHAR(100),              -- e.g. network_timeout, oom, io_spike
    failure_count   INTEGER NOT NULL DEFAULT 0,
    anomaly_count   INTEGER NOT NULL DEFAULT 0,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failure_counts_window
    ON aiops.failure_counts (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_failure_counts_component
    ON aiops.failure_counts (component, window_start);

-- ── Table 2: component_health ─────────────────────────────────────────────────
-- Tracks rolling health scores per service/node. Lower score = more failures.

CREATE TABLE IF NOT EXISTS aiops.component_health (
    id              BIGSERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    component       VARCHAR(255) NOT NULL,
    node_id         VARCHAR(255),
    health_score    NUMERIC(5,2) NOT NULL,     -- 0.00 (critical) to 100.00 (healthy)
    total_events    INTEGER NOT NULL DEFAULT 0,
    error_events    INTEGER NOT NULL DEFAULT 0,
    anomaly_events  INTEGER NOT NULL DEFAULT 0,
    error_rate      NUMERIC(8,6),              -- error_events / total_events
    status          VARCHAR(20) NOT NULL DEFAULT 'UNKNOWN'  -- HEALTHY/DEGRADED/CRITICAL/UNKNOWN
);

CREATE INDEX IF NOT EXISTS idx_component_health_time
    ON aiops.component_health (snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_component_health_component
    ON aiops.component_health (component, snapshot_time DESC);

-- ── Table 3: rolling_error_freq ───────────────────────────────────────────────
-- Rolling 5-minute tumbling window error frequency, used by Grafana trend charts.

CREATE TABLE IF NOT EXISTS aiops.rolling_error_freq (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    total_logs      INTEGER NOT NULL DEFAULT 0,
    error_logs      INTEGER NOT NULL DEFAULT 0,
    anomaly_logs    INTEGER NOT NULL DEFAULT 0,
    error_rate      NUMERIC(8,6),              -- error_logs / total_logs
    anomaly_rate    NUMERIC(8,6),              -- anomaly_logs / total_logs
    avg_severity    VARCHAR(50),               -- modal severity in window
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rolling_error_window
    ON aiops.rolling_error_freq (window_start DESC);

-- ── Table 4: alert_summaries ──────────────────────────────────────────────────
-- One row per anomaly alert published to the critical-alerts Kafka topic.
-- Written by PySpark's hot alerting path (via JDBC) and FastAPI gateway.

CREATE TABLE IF NOT EXISTS aiops.alert_summaries (
    id              BIGSERIAL PRIMARY KEY,
    alert_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    event_timestamp TIMESTAMPTZ NOT NULL,
    component       VARCHAR(255),
    node_id         VARCHAR(255),
    severity        VARCHAR(50),
    alert_code      VARCHAR(100),
    message         TEXT,
    is_anomaly      SMALLINT NOT NULL DEFAULT 1,
    anomaly_score   NUMERIC(8,4),
    model_version   VARCHAR(50),
    acknowledged    BOOLEAN NOT NULL DEFAULT FALSE,
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (alert_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_summaries_triggered
    ON aiops.alert_summaries (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_summaries_component
    ON aiops.alert_summaries (component, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_summaries_severity
    ON aiops.alert_summaries (severity, triggered_at DESC);

-- ── Seed data: Grafana datasource verify ─────────────────────────────────────
INSERT INTO aiops.rolling_error_freq (window_start, window_end, total_logs, error_logs, anomaly_logs, error_rate, anomaly_rate, avg_severity)
VALUES (NOW() - INTERVAL '5 minutes', NOW(), 0, 0, 0, 0.0, 0.0, 'INFO')
ON CONFLICT DO NOTHING;

-- ── Summary comment ───────────────────────────────────────────────────────────
COMMENT ON SCHEMA aiops IS 'AIOps Distributed Log Diagnostics — Analytics schema (Phase 1)';
COMMENT ON TABLE aiops.failure_counts IS 'Aggregated failure counts per component/window — written by PySpark JDBC sync';
COMMENT ON TABLE aiops.component_health IS 'Rolling component health scores — written by PySpark JDBC sync';
COMMENT ON TABLE aiops.rolling_error_freq IS 'Tumbling window error frequencies — Grafana data source';
COMMENT ON TABLE aiops.alert_summaries IS 'Anomaly alert event log — written by PySpark hot path and FastAPI gateway';
