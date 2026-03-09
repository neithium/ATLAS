#!/bin/bash
# =============================================================================
# ATLAS Delta Loader — Startup Wrapper
# =============================================================================
# Waits for PostgreSQL and ClickHouse to be ready, runs init scripts
# (idempotent), then starts the Python loader.
# =============================================================================
set -e

echo "[loader] Waiting for PostgreSQL..."
until pg_isready -h 127.0.0.1 -p 5432 -U atlas 2>/dev/null; do
    sleep 2
done
echo "[loader] PostgreSQL is ready."

echo "[loader] Waiting for ClickHouse..."
until clickhouse-client --host 127.0.0.1 --user "${CLICKHOUSE_USER:-atlas}" --password "${CLICKHOUSE_PASSWORD:-atlas_secure_pwd}" --query "SELECT 1" 2>/dev/null; do
    sleep 2
done
echo "[loader] ClickHouse is ready."

# Run ClickHouse init script (idempotent — uses IF NOT EXISTS)
if [ -f /app/init-scripts/clickhouse-init.sql ]; then
    echo "[loader] Running ClickHouse initialization..."
    clickhouse-client \
        --host 127.0.0.1 \
        --user "${CLICKHOUSE_USER:-atlas}" \
        --password "${CLICKHOUSE_PASSWORD:-atlas_secure_pwd}" \
        --multiquery < /app/init-scripts/clickhouse-init.sql
fi

echo "[loader] Starting Delta Loader..."
exec python3 /app/delta_loader.py
