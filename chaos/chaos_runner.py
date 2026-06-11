#!/usr/bin/env python3
"""
chaos/chaos_runner.py
Phase 5 — Chaos Engineering Orchestrator

Runs all 4 chaos scenarios sequentially and produces a final report:
  5.1  Fluentd crash recovery
  5.2  Kafka controller re-election
  5.3  Delta Lake transaction safety
  5.4  End-to-end latency validation

Usage (from project root):
    python chaos/chaos_runner.py
    python chaos/chaos_runner.py --scenario fluentd
    python chaos/chaos_runner.py --scenario kafka
    python chaos/chaos_runner.py --scenario delta
    python chaos/chaos_runner.py --scenario latency
    python chaos/chaos_runner.py --scenario all
"""

import argparse
import importlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# Add project root to sys.path so 'chaos' can be imported
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "chaos_run.log", mode="a", encoding="utf-8"
        ),
    ],
)
log = logging.getLogger("chaos_runner")

# ── Scenario Registry ──────────────────────────────────────────────────────────
SCENARIOS = {
    "fluentd": {
        "module": "chaos.scenarios.fluentd_crash",
        "label": "5.1 Fluentd Crash Recovery",
    },
    "kafka": {
        "module": "chaos.scenarios.kafka_failover",
        "label": "5.2 Kafka Controller Re-Election",
    },
    "delta": {
        "module": "chaos.scenarios.delta_safety",
        "label": "5.3 Delta Lake Transaction Safety",
    },
    "latency": {
        "module": "chaos.scenarios.latency_probe",
        "label": "5.4 End-to-End Latency Validation",
    },
}


def run_scenario(key: str) -> Dict:
    """Import and execute a named chaos scenario. Returns result dict."""
    meta = SCENARIOS[key]
    log.info("=" * 70)
    log.info("STARTING: %s", meta["label"])
    log.info("=" * 70)
    start = time.time()
    try:
        mod = importlib.import_module(meta["module"])
        result = mod.run()  # every scenario exposes run() -> dict
    except Exception as exc:
        log.exception("Scenario %s raised an unhandled exception: %s", key, exc)
        result = {"passed": False, "error": str(exc), "checks": {}}
    elapsed = round(time.time() - start, 2)
    result["elapsed_s"] = elapsed
    result["label"] = meta["label"]
    status = "✅ PASSED" if result.get("passed") else "❌ FAILED"
    log.info("RESULT: %s  (%ss)\n", status, elapsed)
    return result


def print_report(results: List[Dict]):
    """Print a human-readable summary table."""
    print("\n" + "=" * 70)
    print("  PHASE 5 — CHAOS ENGINEERING REPORT")
    print("  Generated at: " + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 70)
    all_passed = True
    for r in results:
        status = "✅ PASS" if r.get("passed") else "❌ FAIL"
        all_passed = all_passed and bool(r.get("passed"))
        print(f"\n  {status}  {r['label']}  ({r['elapsed_s']}s)")
        checks = r.get("checks", {})
        for check_name, check_val in checks.items():
            icon = "    ✓" if check_val else "    ✗"
            print(f"{icon}  {check_name}")
        if r.get("error"):
            print(f"    ⚠  Error: {r['error']}")
        metrics = r.get("metrics", {})
        for k, v in metrics.items():
            print(f"    →  {k}: {v}")
    print("\n" + "=" * 70)
    overall = "✅ ALL SCENARIOS PASSED" if all_passed else "❌ SOME SCENARIOS FAILED"
    print(f"  OVERALL: {overall}")
    print("=" * 70 + "\n")


def save_report(results: List[Dict], path: Path):
    """Persist results as JSON."""
    path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    log.info("Report saved → %s", path)


def main():
    parser = argparse.ArgumentParser(
        description="Phase 5 Chaos Engineering Runner"
    )
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
        help="Which scenario to run (default: all)",
    )
    parser.add_argument(
        "--report",
        default=str(Path(__file__).parent / "chaos_report.json"),
        help="Path to write JSON report",
    )
    args = parser.parse_args()

    keys = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]

    results = []
    for key in keys:
        results.append(run_scenario(key))

    print_report(results)
    save_report(results, Path(args.report))

    all_passed = all(r.get("passed") for r in results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
