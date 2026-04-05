#!/bin/bash

# Test Livewire Mode
echo "================================================"
echo "ATLAS Livewire Mode Test"
echo "================================================"
echo ""
echo "PIPELINE_MODE=$PIPELINE_MODE"
echo "RUN_PIPELINE=$RUN_PIPELINE"
echo ""

if [ "$PIPELINE_MODE" = "livewire" ]; then
  echo "[✓] Livewire mode is active"
  echo ""
  echo "Checking input directory: /app/data/processed/stream"
  if [ -d "/app/data/processed/stream" ]; then
    echo "[✓] Stream directory exists"
    ls -la /app/data/processed/stream | head -10
  else
    echo "[✗] Stream directory does not exist"
  fi
  echo ""
  echo "Starting livewire pipeline..."
  python3 delta_merge_pipeline.py --input /app/data/processed/stream --output /refined --mode livewire
else
  echo "[✗] ERROR: Not in livewire mode (PIPELINE_MODE=$PIPELINE_MODE)"
  exit 1
fi
