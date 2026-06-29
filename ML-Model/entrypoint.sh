#!/bin/bash

# =====================================================================
# CONFIGURATION
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

while true
do
    # Generate one live snapshot
    python live_data_gen.py --anomalies --anomaly-rate 0.3 --interval $INTERVAL

    # Run prediction on the latest snapshot
    python predict.py

    # Wait before the next cycle
    echo "[INFO] Pipeline cycle complete. Sleeping for ${INTERVAL} seconds before next snapshot..."
    sleep $INTERVAL
done