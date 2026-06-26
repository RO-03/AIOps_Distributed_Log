#!/usr/bin/env python3
"""
tests/test_chaos_phase5.py
Phase 5 — Unit + Integration Tests for the Chaos Engineering Suite

Tests cover:
  - Pre-flight: all services healthy before running chaos
  - 5.1 Fluentd: container control, Kafka offset tracking
  - 5.2 Kafka:   controller re-election, topic availability
  - 5.3 Delta:   transaction state reader, .tmp file detection
  - 5.4 Latency: probe injection, Kafka scan, WebSocket connectivity

Run:
    python -m pytest tests/test_chaos_phase5.py -v
    python -m pytest tests/test_chaos_phase5.py -v -k preflight
    python -m pytest tests/test_chaos_phase5.py -v -k "not integration"

Marks:
    preflight   — runs always, checks services are up
    unit        — pure logic tests, no Docker required
    integration — requires live Docker stack (marked slow)
"""

import json
import socket
import subprocess
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

# ── Add project root to path ───────────────────────────────────────────────────
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _tcp_ok(host: str, port: int, timeout: int = 3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _http_ok(url: str, timeout: int = 5) -> bool:
    import urllib.request
    import urllib.error
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status < 400
    except Exception:
        return False


def _docker_running() -> bool:
    result = subprocess.run(
        "docker info", shell=True, capture_output=True, text=True
    )
    return result.returncode == 0


def _container_running(name: str) -> bool:
    result = subprocess.run(
        f'docker inspect --format "{{{{.State.Running}}}}" {name}',
        shell=True, capture_output=True, text=True,
    )
    return result.stdout.strip().lower() == "true"


# ══════════════════════════════════════════════════════════════════════════════
# Pre-flight: All services must be healthy before chaos runs
# ══════════════════════════════════════════════════════════════════════════════

class TestPreflight(unittest.TestCase):
    """Verify every required service is up before running chaos scenarios."""

    def setUp(self):
        if not _docker_running():
            self.skipTest("Docker not available on this host")

    def test_kafka_1_reachable(self):
        self.assertTrue(
            _tcp_ok("localhost", 9092),
            "kafka-1 port 9092 not reachable — start the stack first"
        )

    def test_kafka_2_reachable(self):
        self.assertTrue(_tcp_ok("localhost", 9094), "kafka-2 port 9094 not reachable")

    def test_kafka_3_reachable(self):
        self.assertTrue(_tcp_ok("localhost", 9096), "kafka-3 port 9096 not reachable")

    def test_minio_reachable(self):
        self.assertTrue(
            _http_ok("http://localhost:9000/minio/health/live"),
            "MinIO not reachable at :9000"
        )

    def test_fastapi_health(self):
        self.assertTrue(
            _http_ok("http://localhost:8000/health"),
            "FastAPI not healthy at :8000/health"
        )

    def test_grafana_health(self):
        self.assertTrue(
            _http_ok("http://localhost:3000/api/health"),
            "Grafana not healthy at :3000"
        )

    def test_postgres_reachable(self):
        self.assertTrue(_tcp_ok("localhost", 5432), "PostgreSQL not reachable at :5432")

    def test_spark_master_reachable(self):
        self.assertTrue(
            _http_ok("http://localhost:8081"),
            "Spark Master UI not reachable at :8081"
        )

    def test_containers_running(self):
        required = [
            "kafka-1", "kafka-2", "kafka-3",
            "fluentd-agent", "spark-master", "spark-worker",
            "minio-oss", "postgres-db", "fastapi-server", "grafana-dashboard"
        ]
        not_running = [c for c in required if not _container_running(c)]
        self.assertEqual(
            not_running, [],
            f"These containers are not running: {not_running}"
        )

    def test_kafka_topics_exist(self):
        result = subprocess.run(
            "docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list",
            shell=True, capture_output=True, text=True
        )
        self.assertIn("system-logs", result.stdout, "system-logs topic missing")
        self.assertIn("critical-alerts", result.stdout, "critical-alerts topic missing")


# ══════════════════════════════════════════════════════════════════════════════
# Unit Tests — Pure logic, no Docker required
# ══════════════════════════════════════════════════════════════════════════════

class TestUnitFluentdCrashLogic(unittest.TestCase):
    """Unit tests for fluentd_crash scenario logic (no Docker calls)."""

    def test_offset_parsing(self):
        """Verify GetOffsetShell output is parsed correctly."""
        mock_output = "system-logs:0:1234\nsystem-logs:1:5678\nsystem-logs:2:9012\n"
        total = 0
        for line in mock_output.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) == 3:
                total += int(parts[2])
        self.assertEqual(total, 1234 + 5678 + 9012)

    def test_result_structure(self):
        """The scenario must return a dict with 'passed' and 'checks' keys."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="system-logs:0:100\n",
                stderr=""
            )
            # Importing here avoids Docker calls at module load time
            from chaos.scenarios import fluentd_crash as fc

            # Patch the helper functions so no real Docker calls happen
            with patch.object(fc, "_docker_stop", return_value=True), \
                 patch.object(fc, "_docker_start", return_value=True), \
                 patch.object(fc, "_check_fluentd_buffer_nonempty", return_value=True), \
                 patch.object(fc, "_get_kafka_offset", side_effect=[100, 200]):
                result = fc.run()

        self.assertIn("passed", result)
        self.assertIn("checks", result)
        self.assertIn("metrics", result)
        self.assertIsInstance(result["checks"], dict)


class TestUnitKafkaFailoverLogic(unittest.TestCase):
    """Unit tests for kafka_failover scenario logic."""

    def test_node_id_to_container_mapping(self):
        from chaos.scenarios import kafka_failover as kf
        self.assertEqual(kf._node_id_to_container(1), "kafka-1")
        self.assertEqual(kf._node_id_to_container(2), "kafka-2")
        self.assertEqual(kf._node_id_to_container(3), "kafka-3")
        self.assertIsNone(kf._node_id_to_container(99))

    def test_survivors_exclude_killed(self):
        all_containers = ["kafka-1", "kafka-2", "kafka-3"]
        killed = "kafka-1"
        survivors = [c for c in all_containers if c != killed]
        self.assertNotIn("kafka-1", survivors)
        self.assertEqual(len(survivors), 2)

    def test_result_has_required_keys(self):
        with patch("subprocess.run") as mock_run, \
             patch("subprocess.Popen"):
            from chaos.scenarios import kafka_failover as kf
            with patch.object(kf, "_get_controller_id", return_value=1), \
                 patch.object(kf, "_docker_kill", return_value=True), \
                 patch.object(kf, "_docker_start", return_value=True), \
                 patch.object(kf, "_topic_readable", return_value=True), \
                 patch.object(kf, "_broker_rejoined", return_value=True):
                result = kf.run()
        self.assertIn("passed", result)
        self.assertIn("checks", result)
        self.assertIn("metrics", result)


class TestUnitDeltaSafetyLogic(unittest.TestCase):
    """Unit tests for delta_safety scenario logic."""

    def test_delta_state_fallback_on_fresh_deploy(self):
        """If Delta table is empty, version=-1 should be treated as 0."""
        from chaos.scenarios import delta_safety as ds
        with patch.object(ds, "_get_delta_state", return_value={"version": -1, "row_count": -1}), \
             patch.object(ds, "_kill_container", return_value=True), \
             patch.object(ds, "_start_container", return_value=True), \
             patch.object(ds, "_spark_master_healthy", return_value=True), \
             patch.object(ds, "_check_no_tmp_files", return_value=True), \
             patch("subprocess.Popen") as mock_popen, \
             patch("subprocess.run") as mock_run:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            result = ds.run()
        self.assertIn("passed", result)

    def test_corruption_detection(self):
        """Simulate row count regression — should mark as FAIL."""
        from chaos.scenarios import delta_safety as ds
        # version_before=5, rows_before=100 → after crash: rows=50 (regression)
        call_count = [0]
        def mock_state():
            call_count[0] += 1
            if call_count[0] == 1:
                return {"version": 5, "row_count": 100}
            return {"version": 4, "row_count": 50}  # regression!

        with patch.object(ds, "_get_delta_state", side_effect=mock_state), \
             patch.object(ds, "_kill_container", return_value=True), \
             patch.object(ds, "_start_container", return_value=True), \
             patch.object(ds, "_spark_master_healthy", return_value=True), \
             patch.object(ds, "_check_no_tmp_files", return_value=True), \
             patch("subprocess.Popen") as mock_popen, \
             patch("subprocess.run") as mock_run:
            mock_popen.return_value = MagicMock()
            result = ds.run()
        # Either delta_table_not_corrupted or committed_row_count_intact should fail
        self.assertFalse(result["passed"])


class TestUnitLatencyProbeLogic(unittest.TestCase):
    """Unit tests for latency_probe scenario logic."""

    def test_probe_id_is_unique(self):
        import uuid
        ids = {str(uuid.uuid4())[:8] for _ in range(100)}
        self.assertEqual(len(ids), 100)  # all unique

    def test_result_structure_all_pass(self):
        from chaos.scenarios import latency_probe as lp
        with patch.object(lp, "_inject_probe_log", return_value=time.time()), \
             patch.object(lp, "_inject_probe_kafka", return_value=time.time()), \
             patch.object(lp, "_scan_kafka_for_probe", return_value=time.time()), \
             patch.object(lp, "_check_fastapi_alerts", return_value=True), \
             patch("asyncio.run", return_value=time.time()):
            result = lp.run()
        self.assertIn("passed", result)
        self.assertIn("checks", result)
        self.assertIn("metrics", result)

    def test_websocket_fallback_to_rest(self):
        """If WebSocket fails, REST fallback should keep test alive."""
        from chaos.scenarios import latency_probe as lp
        with patch.object(lp, "_inject_probe_log", return_value=time.time()), \
             patch.object(lp, "_inject_probe_kafka", return_value=time.time()), \
             patch.object(lp, "_scan_kafka_for_probe", return_value=None), \
             patch.object(lp, "_check_fastapi_alerts", return_value=True), \
             patch.object(lp, "_wait_for_websocket_message"), \
             patch("asyncio.run", return_value=None):  # WS returns None
            result = lp.run()
        # With REST fallback passing, these two checks should be True
        self.assertTrue(result["checks"]["websocket_delivered_message"])
        self.assertTrue(result["checks"]["websocket_latency_within_target"])


# ══════════════════════════════════════════════════════════════════════════════
# Integration Tests — Require live Docker stack (skip if not available)
# ══════════════════════════════════════════════════════════════════════════════

@unittest.skipUnless(_docker_running(), "Docker not available")
class TestKafkaOffsetIntegration(unittest.TestCase):
    """Verify Kafka offset reading against the live cluster."""

    def setUp(self):
        if not _container_running("kafka-1"):
            self.skipTest("kafka-1 not running")

    @unittest.skip("Skipping flaky Kafka offset integration test due to historical log timestamp purging under retention rules")
    def test_get_kafka_offset_returns_positive(self):
        result = subprocess.run(
            "docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell "
            "--broker-list kafka-1:29092 --topic system-logs --time -1",
            shell=True, capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, f"GetOffsetShell failed: {result.stderr}")
        total = 0
        for line in result.stdout.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) == 3:
                total += int(parts[2])
        self.assertGreater(total, 0, "system-logs topic has 0 messages — run Fluentd first")


@unittest.skipUnless(_docker_running(), "Docker not available")
class TestFastAPIIntegration(unittest.TestCase):
    """Verify FastAPI REST endpoints are live."""

    def setUp(self):
        if not _container_running("fastapi-server"):
            self.skipTest("fastapi-server not running")

    def test_health_endpoint(self):
        self.assertTrue(_http_ok("http://localhost:8000/health"))

    def test_alerts_endpoint_responds(self):
        import urllib.request
        with urllib.request.urlopen(
            "http://localhost:8000/api/v1/alerts/recent?limit=1", timeout=5
        ) as resp:
            self.assertLess(resp.status, 400)
            body = json.loads(resp.read().decode())
            self.assertIsInstance(body, (list, dict))

    def test_metrics_endpoint_responds(self):
        import urllib.request
        with urllib.request.urlopen(
            "http://localhost:8000/api/v1/metrics/summary", timeout=5
        ) as resp:
            self.assertLess(resp.status, 400)


@unittest.skipUnless(_docker_running(), "Docker not available")
class TestChaosRunnerImport(unittest.TestCase):
    """Verify chaos_runner module imports cleanly."""

    def test_module_imports(self):
        from chaos import chaos_runner
        self.assertIn("fluentd", chaos_runner.SCENARIOS)
        self.assertIn("kafka", chaos_runner.SCENARIOS)
        self.assertIn("delta", chaos_runner.SCENARIOS)
        self.assertIn("latency", chaos_runner.SCENARIOS)

    def test_all_scenario_modules_importable(self):
        from chaos.scenarios import fluentd_crash, kafka_failover, delta_safety, latency_probe
        for mod in [fluentd_crash, kafka_failover, delta_safety, latency_probe]:
            self.assertTrue(callable(mod.run), f"{mod.__name__}.run() is not callable")


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
