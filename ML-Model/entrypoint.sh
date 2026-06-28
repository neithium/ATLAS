#!/bin/bash

# =====================================================================
# CONFIGURATION
# Set the delay (in seconds) between live inference snapshots.
# Default is 3600 seconds (1 hour). Change this value to speed it up!
# =====================================================================
INTERVAL=${LIVE_GEN_INTERVAL:-300}

echo "=============================================================="
echo "             ATLAS ML MODEL PIPELINE ENGINE               "
echo "=============================================================="
echo "[INFO] Container initialized successfully."
echo "[INFO] Persistent Parquet volumes mounted at /app/telemetry-data"
echo "[INFO] Health checks activated."
echo "[INFO] Commencing automatic live inference simulation..."
echo "[INFO] Snapshot Interval: Every ${INTERVAL} seconds."
echo "--------------------------------------------------------------"

# Execute the live data generator automatically in continuous loop mode.
# (Note: stdout is natively unbuffered because of PYTHONUNBUFFERED=1 in the Dockerfile)
python live_data_gen.py --loop --anomalies --anomaly-rate 0.3 --interval $INTERVAL
