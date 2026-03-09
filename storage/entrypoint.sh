#!/bin/bash
# =============================================================================
# ATLAS Analytics Container — Entrypoint
# =============================================================================
# Initializes PostgreSQL and ClickHouse on first run, then starts supervisord.
# =============================================================================
set -e

PGDATA="/var/lib/postgresql/data"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ATLAS Analytics Hub — ClickHouse + PostgreSQL + Loader"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ---------- PostgreSQL Initialization (first run only) ----------
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[entrypoint] Initializing PostgreSQL data directory..."

    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    su -c "/usr/lib/postgresql/16/bin/initdb -D $PGDATA" postgres

    # Configure authentication
    cat > "$PGDATA/pg_hba.conf" <<EOF
local   all   all                 trust
host    all   all   127.0.0.1/32  md5
host    all   all   ::1/128       md5
EOF

    # Listen on localhost only
    echo "listen_addresses = '127.0.0.1'" >> "$PGDATA/postgresql.conf"
    echo "port = 5432" >> "$PGDATA/postgresql.conf"

    # Start temporarily to create user and database
    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w start" postgres

    su -c "psql -c \"CREATE USER atlas WITH PASSWORD '${POSTGRES_PASSWORD:-atlas_secure_pwd}';\"" postgres
    su -c "psql -c \"CREATE DATABASE atlas_metadata OWNER atlas;\"" postgres
    su -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE atlas_metadata TO atlas;\"" postgres

    # Run init script
    if [ -f /app/init-scripts/postgres-init.sql ]; then
        echo "[entrypoint] Running PostgreSQL init script..."
        su -c "psql -U atlas -d atlas_metadata -f /app/init-scripts/postgres-init.sql" postgres
    fi

    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w stop" postgres
    echo "[entrypoint] PostgreSQL initialization complete."
fi

# ---------- ClickHouse Data Directory ----------
mkdir -p /var/lib/clickhouse /var/lib/clickhouse/preprocessed_configs /var/log/clickhouse-server
chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server

# ---------- Generate ClickHouse user config from environment ----------
mkdir -p /etc/clickhouse-server/users.d
cat > /etc/clickhouse-server/users.d/atlas.xml <<EOF
<clickhouse>
    <users>
        <atlas>
            <password>${CLICKHOUSE_PASSWORD:-atlas_secure_pwd}</password>
            <access_management>1</access_management>
            <networks>
                <ip>127.0.0.1</ip>
                <ip>::1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </atlas>
    </users>
</clickhouse>
EOF

# ---------- Fix volume ownership on every start ----------
# Docker named volumes may be owned by root; services need their dirs.
chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server
chown -R postgres:postgres /var/lib/postgresql/data

echo "[entrypoint] Starting supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/atlas.conf
