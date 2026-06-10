# AIOps Distributed Log Diagnostics & Failure Prediction Platform

## Project Overview

In cloud-native architectures, large-scale microservice clusters generate immense volumes of unstructured, high-velocity log streams. Manually parsing these logs to identify root causes of cluster degradation or predict impending failures becomes impossible at scale.

This project implements a fully containerized, distributed, and completely decoupled **AIOps Lakehouse & Streaming Architecture**. It streams real-world log entries using the **Loghub BGL Supercomputer Dataset** (or HDFS logs), structures them in real time, and applies unsupervised NLP-based machine learning models to detect operational anomalies.

To maximize scalability and performance, the platform separates long-term analytical storage from real-time alerting using an **Event-Driven Two-Tier Architecture**.

---

## Architecture Overview

### 1. Lakehouse Path (Cold/Warm Analytics)

The compute layer commits the entire structured stream to **MinIO Object Storage** using the **Delta Lake** table format.

Features:

* ACID-compliant transactions
* Schema enforcement
* Time-travel debugging
* Historical analytics storage

Aggregated metrics are continuously synchronized to **PostgreSQL**, which serves as the data source for **Grafana dashboards**.

---

### 2. Event-Driven Alerting Path (Hot Stream)

When an anomaly is detected:

1. PySpark publishes an alert event to a dedicated Kafka topic:

   * `critical-alerts`

2. A FastAPI background worker consumes these events asynchronously.

3. Alert events are immediately pushed to clients through WebSockets.

Benefits:

* No database polling
* Low-latency alert delivery
* Fully asynchronous event processing

---

# Architectural Topology

```text
[Log Replay Engine Container]
              │ (Appends to shared storage volume)
              ▼
[Fluentd Aggregator Container]
              │ (Async Disk Buffering & Network Ingestion)
              ▼
[Apache Kafka Cluster (3 Brokers via KRaft)]
       │      ▲
       │      │ (Publish 'critical-alerts' Event)
       │      └──────────────────────────────────────────────┐
       ▼ (Read 'system-logs' Topic)                         │
[PySpark Compute Engine (Structured Streaming)]             │
       ├────────────────────────────────────────────────────┤
       ▼ (Cold Path: Delta Lake Format)                     │
[MinIO S3 Object Storage Data Lake]                         │
       │                                                    │
       ▼ (Micro-Batch Sync)                                 │
[PostgreSQL Analytical DB]                                  │
       │                                                    │
       ▼ (Historical Charts)                                │
[Grafana Metrics Analytics]                                 │
                                                            ▼
                                              (Read 'critical-alerts')
                                              [FastAPI Serving Gateway]
                                                            │
                                                            ▼
                                               (WebSocket Broadcast)
                                           [Live Anomaly Alerts UI]
```

---

# Container Inventory & Network Segmentation

The deployment is divided into three isolated Docker bridge networks.

| Network           | Containers                                                     | Ports / Protocols            | Purpose                                                |
| ----------------- | -------------------------------------------------------------- | ---------------------------- | ------------------------------------------------------ |
| **ingestion-net** | log-generator, fluentd-agent, kafka-1, kafka-2, kafka-3        | TCP 24224, 9092, 9093        | High-throughput log ingestion and broker communication |
| **storage-net**   | kafka-1..3, spark-master, spark-worker, minio-oss, postgres-db | S3A, JDBC, 9000, 5432, 7077  | Storage and distributed computation                    |
| **serving-net**   | kafka-1..3, fastapi-server, grafana-dashboard                  | HTTP, WebSockets, 8000, 3000 | Real-time serving and visualization                    |

---

# Project Execution Plan

# Phase 1: Docker Environment & Lakehouse Orchestration

## Goal

Provision all infrastructure components required for a cloud-native AIOps platform.

---

## Step 1.1 — Multi-Network Docker Initialization

Build a master Docker Compose configuration containing:

* Kafka Cluster (3 Brokers)
* Fluentd
* Log Generator
* Spark Master
* Spark Worker
* MinIO
* PostgreSQL
* FastAPI
* Grafana

Requirements:

* Three isolated bridge networks
* Persistent Docker volumes
* Service discovery via Docker DNS

---

## Step 1.2 — ZooKeeperless Kafka Cluster (KRaft)

Configure Kafka 3.6+ in KRaft mode.

Requirements:

* Three brokers
* Unique node IDs
* Dedicated controller quorum
* Replication factor = 3
* No ZooKeeper

---

## Step 1.3 — Storage Engine Provisioning

### MinIO

Create bucket:

```text
telemetry-lakehouse
```

### PostgreSQL

Create analytics tables for:

* Failure counts
* Component health
* Rolling error frequencies
* Alert summaries

---

## Step 1.4 — Network Isolation Audit

Validate:

* Log Generator cannot access PostgreSQL
* Log Generator cannot access FastAPI
* Cross-network communication only where intended

---

# Phase 2: Fluentd Logging Pipeline & Ingestion

## Goal

Move raw telemetry logs into Kafka safely and reliably.

---

## Step 2.1 — Dataset Engineering

Mount:

```text
Loghub BGL Supercomputer Dataset
```

Characteristics:

* 4.7 million records
* Real-world failure events
* Labeled anomaly patterns

---

## Step 2.2 — Continuous Append Engine

Develop a Python replay service:

Behavior:

```python
Read Dataset Line
      ↓
Append To Active Log File
      ↓
Sleep 0.001 Seconds
      ↓
Repeat
```

Purpose:

Simulate live infrastructure logs.

---

## Step 2.3 — Fluentd Parsing Pipeline

Configure Fluentd:

* Tail live file
* Parse raw logs
* Buffer on disk
* Forward to Kafka

Requirements:

* Persistent chunk storage
* Memory protection
* Automatic retry behavior

---

## Step 2.4 — Kafka Topic Design

### system-logs

```text
Partitions: 3
Replication Factor: 3
```

Purpose:

Bulk log ingestion.

### critical-alerts

```text
Partitions: 1
Replication Factor: 3
```

Purpose:

Low-latency alert delivery.

Producer Requirements:

```text
acks=all
```

---

# Phase 3: Distributed PySpark Compute & ML Inference

## Goal

Transform raw logs into structured intelligence.

---

## Step 3.1 — Regex Schema-on-Read

Parse raw logs into:

```text
[
  alert_code,
  timestamp,
  component,
  message,
  severity
]
```

Using:

```python
regexp_extract()
```

---

## Step 3.2 — NLP Feature Engineering

Spark ML Pipeline:

```text
Tokenizer
      ↓
HashingTF
      ↓
IDF
      ↓
Feature Vector
```

Purpose:

Convert free-text logs into numerical representations.

---

## Step 3.3 — Unsupervised Machine Learning

### Offline Training

Train baseline model using healthy logs.

Options:

* K-Means Clustering
* Isolation Forest

---

### Streaming Inference

For each micro-batch:

```text
Input Log
      ↓
Feature Vector
      ↓
Model Score
      ↓
is_anomaly
```

Output:

```text
0 = Normal
1 = Anomaly
```

---

## Step 3.4 — Dual Egress Bifurcation

Use:

```python
foreachBatch()
```

---

### Cold Analytics Path

Store all processed logs in:

```text
MinIO
    +
Delta Lake
```

Partitioned by:

```text
date
component
```

---

### Hot Alerting Path

Filter:

```python
is_anomaly == 1
```

Publish to:

```text
critical-alerts
```

JSON Structure:

```json
{
  "timestamp": "...",
  "component": "...",
  "severity": "...",
  "message": "..."
}
```

---

### PostgreSQL Aggregation Path

Store:

* Error counts
* Alert frequencies
* Component statistics

via JDBC.

---

# Phase 4: FastAPI Async Gateway & Grafana

## Goal

Serve real-time alerts and historical analytics.

---

## Step 4.1 — Kafka Background Consumer

Use:

```python
aiokafka
```

FastAPI startup event launches:

```text
Persistent Async Consumer
```

Listening to:

```text
critical-alerts
```

---

## Step 4.2 — WebSocket Alert Broker

Endpoint:

```http
/ws/v1/live-alerts
```

Workflow:

```text
Kafka Event
      ↓
Background Consumer
      ↓
WebSocket Broadcast
      ↓
Browser Client
```

No polling required.

---

## Step 4.3 — Historical Analytics APIs

Example Endpoint:

```http
GET /api/v1/metrics/summary
```

Source:

```text
PostgreSQL
```

Returns:

* Error trends
* Failure counts
* Component health metrics

---

## Step 4.4 — Grafana Visualization

Data Source:

```text
PostgreSQL
```

Dashboards:

* Error trend charts
* Failure frequency analysis
* Component health heatmaps
* Historical anomaly timelines

---

# Phase 5: Chaos Engineering & Fault Validation

## Goal

Validate resilience under real-world failures.

---

## Step 5.1 — Fluentd Crash Recovery

Scenario:

```text
Kill Fluentd
```

Validation:

* Buffered chunks survive restart
* No log loss
* Backlog replay succeeds

---

## Step 5.2 — Kafka Controller Re-Election

Scenario:

```bash
docker kill kafka-1
```

Validation:

* New leader elected
* Consumers remain active
* No message corruption

---

## Step 5.3 — Delta Lake Transaction Safety

Scenario:

```text
Terminate Spark During Write
```

Validation:

* No partial data committed
* Delta transaction rollback succeeds

---

## Step 5.4 — End-to-End Latency Validation

Workflow:

```text
Log Generated
      ↓
Fluentd
      ↓
Kafka
      ↓
Spark
      ↓
Anomaly Detection
      ↓
critical-alerts
      ↓
FastAPI
      ↓
WebSocket
      ↓
Browser
```

Target:

```text
< 100 ms
```

alert propagation latency.

---

# Technology Stack

## Dataset

* Loghub BGL Supercomputer Dataset

---

## Programming Languages

* Python 3.11+
* PySpark 3.5+

---

## Streaming Infrastructure

* Apache Kafka 3.6+
* Kafka KRaft Mode
* Fluentd 1.16+

---

## Lakehouse Technologies

* Delta Lake
* MinIO
* S3A Connector

---

## Databases

* PostgreSQL 16+

---

## API Layer

* FastAPI
* WebSockets
* AsyncIO

---

## Visualization

* Grafana 10+

---

## Machine Learning

* Spark MLlib
* K-Means
* Isolation Forest
* NLP Feature Engineering

---

## Required Python Dependencies

```text
aiokafka
asyncpg
websockets
delta-spark
pyspark
pyspark-sql-kafka
```

---

# Expected Deliverables

* Multi-network Docker Compose deployment
* 3-node Kafka KRaft cluster
* Fluentd ingestion pipeline
* Structured Streaming PySpark engine
* NLP anomaly detection model
* Delta Lakehouse on MinIO
* PostgreSQL analytical database
* FastAPI async alerting service
* WebSocket real-time notification system
* Grafana operational dashboards
* Chaos engineering validation suite
* End-to-end AIOps observability platform
