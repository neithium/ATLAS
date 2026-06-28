# =============================================================================
# ATLAS ML Inference Pipeline — Dockerfile
# =============================================================================
# Minimal Python 3.11 slim image.
#
# Volumes expected at runtime (set in docker-compose.yml):
#   /data/live            → read-only  — live telemetry Parquet input
#   /data/ml_predictions  → read-write — enriched Parquet output
#   /app/models           → read-only  — isolation_forest.pkl
# =============================================================================

FROM python:3.11-slim

# --- System dependencies (none needed beyond Python stdlib + pip) -----------
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# --- Working directory -------------------------------------------------------
WORKDIR /app

# --- Python dependencies (cached layer) --------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Application code --------------------------------------------------------
COPY config.py       .
COPY health_score.py .
COPY inference.py    .

# --- Runtime directories (created here so they exist even without volume mounts)
RUN mkdir -p /data/live /data/ml_predictions /app/models

# --- Default environment -------------------------------------------------------
ENV ML_INPUT_DIR=/data/live \
    ML_OUTPUT_DIR=/data/ml_predictions \
    ML_MODEL_PATH=/app/models/isolation_forest.pkl \
    ML_PREPROCESSOR_PATH=/app/models/preprocessor.pkl \
    ML_HEALTH_CONFIG_PATH=/app/models/health_score_config.pkl \
    ML_WATCH_MODE=true \
    ML_POLL_INTERVAL=60 \
    PYTHONUNBUFFERED=1

# --- Health check — verifies dependencies are importable ----------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import inference; print('OK')" || exit 1

# --- Entrypoint ---------------------------------------------------------------
# One-shot by default; set ML_WATCH_MODE=true in docker-compose for continuous.
CMD ["python", "inference.py"]
