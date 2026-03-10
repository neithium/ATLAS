#!/bin/bash
# =============================================================================
# ATLAS Delta Loader — Startup Wrapper
# =============================================================================
# Waits for PostgreSQL and ClickHouse to be ready, runs init scripts
# (idempotent), then starts the Python loader in scheduler mode.
#
# ClickHouse init uses the 'default' user (no password on localhost) because
# the XML-configured 'atlas' user may not be loaded yet when ClickHouse first
# starts. The Python loader itself connects as the 'atlas' user afterwards.
#
# SCHEDULE_INTERVAL_SECONDS (from .env.example, default 3600):
#   The loader runs once, sleeps for this many seconds, then repeats.
#   3600 = 1 hour, matching the upstream hourly batch cadence.
#   Set to 0 for a single one-shot run.
# =============================================================================
set -e

MAX_WAIT=120   # seconds before giving up on a dependency
SLEEP_STEP=2   # seconds between retries

# ── Wait for PostgreSQL ─────────────────────────────────────────────────────
echo "[loader] Waiting for PostgreSQL (max ${MAX_WAIT}s)..."
elapsed=0
until pg_isready -h 127.0.0.1 -p 5432 -U "${POSTGRES_USER:-atlas}" 2>/dev/null; do
    sleep $SLEEP_STEP
    elapsed=$((elapsed + SLEEP_STEP))
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "[loader] ERROR: PostgreSQL not ready after ${MAX_WAIT}s — aborting."
        exit 1
    fi
done
echo "[loader] PostgreSQL is ready (waited ${elapsed}s)."

# ── Wait for ClickHouse ─────────────────────────────────────────────────────
# Use the 'default' user (no auth on localhost) for the health probe.
# The XML-configured 'atlas' user may still be loading at this point.
echo "[loader] Waiting for ClickHouse (max ${MAX_WAIT}s)..."
elapsed=0
until clickhouse-client --host 127.0.0.1 --query "SELECT 1" 2>/dev/null; do
    sleep $SLEEP_STEP
    elapsed=$((elapsed + SLEEP_STEP))
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "[loader] ERROR: ClickHouse not ready after ${MAX_WAIT}s — aborting."
        exit 1
    fi
done
echo "[loader] ClickHouse is ready (waited ${elapsed}s)."

# ── Run ClickHouse DDL init (idempotent — IF NOT EXISTS) ────────────────────
# Uses 'default' user so we never fail because the 'atlas' XML user
# hasn't been loaded yet. 'default' has full DDL rights on localhost.
if [ -f /app/init-scripts/clickhouse-init.sql ]; then
    echo "[loader] Running ClickHouse initialization (default user)..."
    if clickhouse-client --host 127.0.0.1 --multiquery \
         < /app/init-scripts/clickhouse-init.sql; then
        echo "[loader] ClickHouse schema initialised successfully."
    else
        echo "[loader] ERROR: ClickHouse init SQL failed (exit $?)."
        echo "[loader] Dumping init script for debugging:"
        cat /app/init-scripts/clickhouse-init.sql
        exit 1
    fi
else
    echo "[loader] WARNING: /app/init-scripts/clickhouse-init.sql not found — skipping."
fi

# ── Verify atlas database exists ────────────────────────────────────────────
if clickhouse-client --host 127.0.0.1 --query "SHOW DATABASES" 2>/dev/null | grep -q "^atlas$"; then
    echo "[loader] Verified: 'atlas' database exists."
    TABLE_COUNT=$(clickhouse-client --host 127.0.0.1 --query "SELECT count() FROM system.tables WHERE database = 'atlas'" 2>/dev/null)
    echo "[loader] Tables in atlas DB: ${TABLE_COUNT}"
else
    echo "[loader] ERROR: 'atlas' database not found after init — check init SQL."
    exit 1
fi

# ── Check refined data volume ───────────────────────────────────────────────
REFINED="${REFINED_DATA_PATH:-/data/refined}"
if [ -d "$REFINED" ]; then
    FILE_COUNT=$(find "$REFINED" -name "*.parquet" 2>/dev/null | head -100 | wc -l)
    echo "[loader] Refined data path: $REFINED (${FILE_COUNT} parquet file(s) found)"
else
    echo "[loader] NOTE: Refined data path $REFINED does not exist yet."
    echo "[loader]       Loader will wait for Lakehouse to produce data."
fi

echo "[loader] Starting Delta Loader (SCHEDULE_INTERVAL_SECONDS=${SCHEDULE_INTERVAL_SECONDS:-0})..."
exec python3 /app/delta_loader.py
