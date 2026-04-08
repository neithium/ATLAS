#!/bin/bash

# Test Livewire Modes with atlas-lakehouse Container
# ====================================================

cd "$(dirname "$0")" || exit 1

echo "=========================================="
echo "ATLAS Livewire Mode Testing"
echo "=========================================="
echo ""

# Test 1: Benchmark Mode (Default)
echo "[TEST 1] Running atlas-lakehouse in BENCHMARK mode..."
echo "Command: docker-compose run -e PIPELINE_MODE=benchmark -e RUN_GENERATOR=y -e RUN_PIPELINE=y atlas-lakehouse"
echo ""

docker-compose run --rm \
  -e PIPELINE_MODE=benchmark \
  -e RUN_GENERATOR=y \
  -e RUN_PIPELINE=y \
  -e DEVICE_COUNT=5 \
  -e BATCH_SIZE=500 \
  -e NUM_DAYS=2 \
  atlas-lakehouse

echo ""
echo "[TEST 1] Completed"
echo ""
echo "=========================================="
echo ""

# Test 2: Livewire Mode (Streaming)
echo "[TEST 2] Running atlas-lakehouse in LIVEWIRE mode..."
echo "Command: docker-compose run -e PIPELINE_MODE=livewire -e RUN_GENERATOR=n -e RUN_PIPELINE=y atlas-lakehouse"
echo ""

docker-compose run --rm \
  -e PIPELINE_MODE=livewire \
  -e RUN_GENERATOR=n \
  -e RUN_PIPELINE=y \
  atlas-lakehouse

echo ""
echo "[TEST 2] Completed"
echo ""
echo "=========================================="
echo "Done!"
