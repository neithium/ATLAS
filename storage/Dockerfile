# =============================================================================
# ATLAS Analytics Container — Unified Dockerfile
# =============================================================================
# Runs ClickHouse + PostgreSQL + Delta Loader in a single container
# managed by supervisord. Part of the 5-container ATLAS architecture.
#
# Container 5: atlas-analytics
#   - ClickHouse: Fast analytical queries on deduplicated telemetry data
#   - PostgreSQL: Metadata store (watermarks, device/location registry)
#   - Delta Loader: Batch ingestion from Refined Layer (partition-aware)
# =============================================================================

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# ---------- Base tools ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 ca-certificates lsb-release supervisor \
    python3 python3-pip postgresql-common locales \
    && sed -i 's/# en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen \
    && locale-gen \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# ---------- ClickHouse ----------
RUN curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
    | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] \
    https://packages.clickhouse.com/deb stable main" \
    > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       clickhouse-server clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

# ---------- PostgreSQL 16 ----------
RUN echo "deb [signed-by=/usr/share/keyrings/pgdg-keyring.gpg] \
    http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor -o /usr/share/keyrings/pgdg-keyring.gpg \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16 \
    && rm -rf /var/lib/apt/lists/*

# ---------- Python dependencies (build-time, not runtime) ----------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# ---------- Application files ----------
COPY supervisord.conf /etc/supervisor/conf.d/atlas.conf
COPY entrypoint.sh /app/entrypoint.sh
COPY loader-start.sh /app/loader-start.sh
RUN chmod +x /app/entrypoint.sh /app/loader-start.sh

# ---------- Directories ----------
RUN mkdir -p /app/init-scripts /var/lib/clickhouse /var/lib/postgresql/data \
    && chown clickhouse:clickhouse /var/lib/clickhouse \
    && chown postgres:postgres /var/lib/postgresql/data

# ---------- Ports ----------
# 8123: ClickHouse HTTP | 9000: ClickHouse Native TCP | 5432: PostgreSQL
EXPOSE 8123 9000 5432

ENTRYPOINT ["/app/entrypoint.sh"]
