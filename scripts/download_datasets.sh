#!/usr/bin/env bash
# =============================================================================
# scripts/download_datasets.sh
# Phase 0 — Download all required datasets for AIOps platform
# =============================================================================

set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
echo "📁 Target directory: $DATA_DIR"
mkdir -p "$DATA_DIR"

# ── 1. HDFS Log Dataset (Loghub / Zenodo) ────────────────────────────────────
echo ""
echo "⬇️  [1/3] Downloading HDFS Log Dataset..."
HDFS_URL="https://raw.githubusercontent.com/logpai/loghub/master/HDFS/HDFS_2k.log"
curl -L --progress-bar -o "$DATA_DIR/HDFS_2k.log" "$HDFS_URL" \
  && echo "    ✅ HDFS_2k.log saved" \
  || echo "    ⚠️  HDFS download failed — try manual: https://github.com/logpai/loghub"

# ── 2. BGL Supercomputer Logs (smaller sample via loghub) ────────────────────
echo ""
echo "⬇️  [2/3] Downloading BGL Log Dataset (2k sample)..."
BGL_URL="https://raw.githubusercontent.com/logpai/loghub/master/BGL/BGL_2k.log"
curl -L --progress-bar -o "$DATA_DIR/BGL_2k.log" "$BGL_URL" \
  && echo "    ✅ BGL_2k.log saved" \
  || echo "    ⚠️  BGL download failed"

# ── 3. Generate synthetic microservice logs ───────────────────────────────────
echo ""
echo "⬇️  [3/3] Generating synthetic microservice failure logs..."
python3 "$(dirname "$0")/generate_logs.py" --output "$DATA_DIR/synthetic_logs.jsonl" --count 50000
echo "    ✅ synthetic_logs.jsonl saved"

echo ""
echo "════════════════════════════════════"
echo "✅ Phase 0 dataset setup complete."
echo "Files in $DATA_DIR:"
ls -lh "$DATA_DIR"
echo "════════════════════════════════════"
