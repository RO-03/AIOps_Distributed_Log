-- docker/postgres/init.sql
-- AIOps PostgreSQL Analytics Schema
-- Phase 1 / Step 1.3 + Phase 3 / Step 3.4 (PostgreSQL Aggregation Path)
--
-- Tables:
--   batch_metrics        — Per-batch component aggregation written by PySpark JDBC
--   anomaly_alerts_pg    — Anomaly events mirrored from Delta Lake
--   component_health     — Rolling health summary (updated by PySpark)
--   alert_summary        — Daily alert frequency summary

-- ── batch_metrics ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS batch_metrics (
    id              BIGSERIAL PRIMARY KEY,
    component       TEXT        NOT NULL,
    log_date        DATE        NOT NULL,
    severity        TEXT,
    total_events    BIGINT      NOT NULL DEFAULT 0,
    anomaly_count   BIGINT      NOT NULL DEFAULT 0,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_batch_metrics_date      ON batch_metrics (log_date DESC);
CREATE INDEX IF NOT EXISTS idx_batch_metrics_component ON batch_metrics (component);

-- ── anomaly_alerts_pg ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_alerts_pg (
    id              BIGSERIAL PRIMARY KEY,
    alert_id        TEXT        NOT NULL UNIQUE,
    event_time      TIMESTAMPTZ,
    component       TEXT,
    severity        TEXT,
    message         TEXT,
    host            TEXT,
    anomaly_score   DOUBLE PRECISION,
    cluster_id      INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_event_time  ON anomaly_alerts_pg (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_component   ON anomaly_alerts_pg (component);

-- ── component_health ─────────────────────────────────────────────────────────
-- Maintained by PySpark (upsert) — one row per component
CREATE TABLE IF NOT EXISTS component_health (
    component           TEXT        PRIMARY KEY,
    total_events        BIGINT      NOT NULL DEFAULT 0,
    total_anomalies     BIGINT      NOT NULL DEFAULT 0,
    error_rate_pct      DOUBLE PRECISION,
    last_seen           TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'unknown',
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── alert_summary ────────────────────────────────────────────────────────────
-- One row per (date, component) — for Grafana dashboards
CREATE TABLE IF NOT EXISTS alert_summary (
    id              BIGSERIAL PRIMARY KEY,
    summary_date    DATE        NOT NULL,
    component       TEXT        NOT NULL,
    alert_count     BIGINT      NOT NULL DEFAULT 0,
    event_count     BIGINT      NOT NULL DEFAULT 0,
    anomaly_rate    DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (summary_date, component)
);

CREATE INDEX IF NOT EXISTS idx_alert_summary_date ON alert_summary (summary_date DESC);

-- ── Seed component_health with known BGL components ──────────────────────────
INSERT INTO component_health (component, status)
VALUES
    ('RAS',     'unknown'),
    ('KERNEL',  'unknown'),
    ('APP',     'unknown'),
    ('MMCS',    'unknown'),
    ('LINKCARD','unknown')
ON CONFLICT (component) DO NOTHING;

-- ── model_metrics ─────────────────────────────────────────────────────────────
-- Stores per-batch classification evaluation metrics for MLOps tracking.
-- Populated by PySpark stream_processor on every micro-batch where
-- label_original (ground truth) is available.
CREATE TABLE IF NOT EXISTS model_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            BIGINT,
    evaluated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    total_count         BIGINT          NOT NULL,
    accuracy            DOUBLE PRECISION NOT NULL,
    precision_score     DOUBLE PRECISION NOT NULL,
    recall_score        DOUBLE PRECISION NOT NULL,
    f1_score            DOUBLE PRECISION NOT NULL,
    true_positives      BIGINT          NOT NULL,
    false_positives     BIGINT          NOT NULL,
    true_negatives      BIGINT          NOT NULL,
    false_negatives     BIGINT          NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_metrics_time ON model_metrics (evaluated_at DESC);
