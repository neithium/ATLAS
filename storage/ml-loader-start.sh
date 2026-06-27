#!/bin/bash
# =============================================================================
# ATLAS ML Loader — Startup Wrapper
# =============================================================================
# Waits for ClickHouse to be ready, verifies the ML predictions table exists,
# then starts the Python ML loader in scheduler mode.
#
# This follows the same pattern as loader-start.sh (the existing delta_loader
# startup wrapper) but is dedicated to the ML inference pipeline.
#
# ML_SCHEDULE_INTERVAL_SECONDS (from .env, default 300 for demo):
#   The loader runs once, sleeps for this many seconds, then repeats.
#   300  = 5 minutes (demo mode, fast feedback)
#   3600 = 1 hour (production, matching upstream batch cadence)
#   0    = one-shot, run once and exit
# =============================================================================
set -e

MAX_WAIT=120   # seconds before giving up on a dependency
SLEEP_STEP=2   # seconds between retries

# ── Wait for ClickHouse ─────────────────────────────────────────────────────
# Use the 'default' user (no auth on localhost) for the health probe.
echo "[ml-loader] Waiting for ClickHouse (max ${MAX_WAIT}s)..."
elapsed=0
until clickhouse-client --host 127.0.0.1 --query "SELECT 1" 2>/dev/null; do
    sleep $SLEEP_STEP
    elapsed=$((elapsed + SLEEP_STEP))
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "[ml-loader] ERROR: ClickHouse not ready after ${MAX_WAIT}s — aborting."
        exit 1
    fi
done
echo "[ml-loader] ClickHouse is ready (waited ${elapsed}s)."

# ── Verify ML predictions table exists ──────────────────────────────────────
if clickhouse-client --host 127.0.0.1 \
   --query "SELECT count() FROM atlas.telemetry_ml_predictions" 2>/dev/null; then
    echo "[ml-loader] Verified: atlas.telemetry_ml_predictions table exists."
else
    echo "[ml-loader] WARNING: ML predictions table not found."
    echo "[ml-loader]   init.sql may not have run yet — the loader will retry."
fi

# ── Check ML predictions data volume ───────────────────────────────────────
ML_PATH="${ML_PREDICTIONS_PATH:-/data/ml_predictions}"
if [ -d "$ML_PATH" ]; then
    FILE_COUNT=$(find "$ML_PATH" -name "*.parquet" 2>/dev/null | head -100 | wc -l)
    echo "[ml-loader] ML predictions path: $ML_PATH (${FILE_COUNT} parquet file(s) found)"
else
    echo "[ml-loader] NOTE: ML predictions path $ML_PATH does not exist yet."
    echo "[ml-loader]       Loader will wait for inference layer to produce data."
fi

echo "[ml-loader] Starting ML Loader (ML_SCHEDULE_INTERVAL_SECONDS=${ML_SCHEDULE_INTERVAL_SECONDS:-0})..."
exec python3 /app/ml_loader.py
