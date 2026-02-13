#!/bin/bash
# Clean script for TVMJNS monorepo

set -e

echo "=== Cleaning TVMJNS Monorepo ==="

# Clean broker
echo "Cleaning broker..."
rm -rf broker/build

# Clean Java SDK
echo "Cleaning sdk-java..."
cd sdk-java
mvn clean -q || true
cd ..

# Clean Python SDK
echo "Cleaning analytics-py..."
rm -rf analytics-py/build
rm -rf analytics-py/dist
rm -rf analytics-py/*.egg-info
find analytics-py -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

echo "✓ Clean complete"
