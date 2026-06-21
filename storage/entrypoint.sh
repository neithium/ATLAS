#!/bin/bash
# =============================================================================
# ATLAS Analytics Container — Entrypoint
# =============================================================================
# Initializes PostgreSQL and ClickHouse on first run, then starts supervisord.
# Credentials come from .env.example via docker-compose env_file directive.
# =============================================================================
set -e

PGDATA="/var/lib/postgresql/data"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ATLAS Analytics Hub — ClickHouse + PostgreSQL + Loader"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Credentials sourced from .env.example"
echo "  POSTGRES_USER  = ${POSTGRES_USER:-atlas}"
echo "  POSTGRES_DB    = ${POSTGRES_DB:-atlas_metadata}"
echo "  CLICKHOUSE_USER= ${CLICKHOUSE_USER:-atlas}"
echo ""

# ---------- PostgreSQL Initialization (first run only) ----------
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[entrypoint] Initializing PostgreSQL data directory..."

    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    su -c "/usr/lib/postgresql/16/bin/initdb -D $PGDATA" postgres

    # Configure authentication:
    #   local (Unix socket)  → trust  (for init scripts)
    #   127.0.0.1            → md5    (loader inside container)
    #   0.0.0.0/0            → md5    (docker-compose exposed port)
    cat > "$PGDATA/pg_hba.conf" <<EOF
local   all   all                 trust
host    all   all   127.0.0.1/32  md5
host    all   all   ::1/128       md5
host    all   all   0.0.0.0/0     md5
EOF

    # Listen on all interfaces so the exposed port (5432) works from host
    echo "listen_addresses = '0.0.0.0'" >> "$PGDATA/postgresql.conf"
    echo "port = 5432" >> "$PGDATA/postgresql.conf"

    # Start temporarily to create user and database
    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w start" postgres

    # Set password for postgres superuser (required for md5 auth over TCP)
    su -c "psql -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD:-atlas_secure_pwd}';\"" postgres

    su -c "psql -c \"CREATE USER ${POSTGRES_USER:-atlas} WITH PASSWORD '${POSTGRES_PASSWORD:-atlas_secure_pwd}';\"" postgres
    su -c "psql -c \"CREATE DATABASE ${POSTGRES_DB:-atlas_metadata} OWNER ${POSTGRES_USER:-atlas};\"" postgres
    su -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB:-atlas_metadata} TO ${POSTGRES_USER:-atlas};\"" postgres

    # Run init script
    if [ -f /app/init-scripts/postgres-init.sql ]; then
        echo "[entrypoint] Running PostgreSQL init script..."
        su -c "psql -U ${POSTGRES_USER:-atlas} -d ${POSTGRES_DB:-atlas_metadata} -f /app/init-scripts/postgres-init.sql" postgres
    fi

    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w stop" postgres
    echo "[entrypoint] PostgreSQL initialization complete."
else
    echo "[entrypoint] PostgreSQL already initialized (PG_VERSION exists)."
fi

# ---------- ClickHouse Data Directory ----------
mkdir -p /var/lib/clickhouse /var/lib/clickhouse/preprocessed_configs /var/log/clickhouse-server
chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server

# Ensure ClickHouse config is readable and preprocessed dir is writable
chmod 644 /etc/clickhouse-server/config.xml 2>/dev/null || true
chown -R clickhouse:clickhouse /etc/clickhouse-server/ 2>/dev/null || true

# ---------- Generate ClickHouse user config from .env.example credentials ---
mkdir -p /etc/clickhouse-server/users.d
cat > /etc/clickhouse-server/users.d/atlas.xml <<EOF
<clickhouse>
    <users>
        <${CLICKHOUSE_USER:-atlas}>
            <password>${CLICKHOUSE_PASSWORD:-atlas_secure_pwd}</password>
            <access_management>1</access_management>
            <networks>
                <ip>::/0</ip>
                <ip>0.0.0.0/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </${CLICKHOUSE_USER:-atlas}>
    </users>
</clickhouse>
EOF

# ---------- ClickHouse: force IPv4-only listening ----------
# The stock config.xml ships listen_host :: (IPv6) which crashes with
# exit 210 when the container has no IPv6 support.
if [ -f /app/override-listen.xml ]; then
    cp /app/override-listen.xml /etc/clickhouse-server/config.d/override-listen.xml
    echo "[entrypoint] Installed ClickHouse IPv4-only override."
fi

# ---------- Fix volume ownership on every start ----------
# Docker named volumes may be owned by root; services need their dirs.
chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server/
chown -R postgres:postgres /var/lib/postgresql/data

# ---------- Fix postgres superuser password on existing installs ----------
# If PG was already initialized without a postgres password, set it now.
if [ -s "$PGDATA/PG_VERSION" ]; then
    echo "[entrypoint] Ensuring postgres superuser has a password..."
    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w start" postgres 2>/dev/null
    su -c "psql -c \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD:-atlas_secure_pwd}';\"" postgres 2>/dev/null
    su -c "/usr/lib/postgresql/16/bin/pg_ctl -D $PGDATA -w stop" postgres 2>/dev/null
fi

# ---------- Clear Streamlit cache on every start ----------
# Ensures latest app.py changes are reflected without browser cache issues
echo "[entrypoint] Clearing Streamlit cache..."
rm -rf ~/.streamlit/cache 2>/dev/null || true
mkdir -p ~/.streamlit
cat > ~/.streamlit/config.toml <<EOF
[client]
showErrorDetails = true

[logger]
level = "info"

[client]
toolbarMode = "minimal"
EOF
echo "[entrypoint] Streamlit cache cleared and config initialized."

echo "[entrypoint] Starting supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/atlas.conf
