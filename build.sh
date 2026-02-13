#!/bin/bash
# Build script for TVMJNS monorepo

set -e

echo "=== Building TVMJNS Monorepo ==="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Build broker
echo -e "${BLUE}Building broker (C++17)...${NC}"
cd broker
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j$(nproc)
cd ../..
echo -e "${GREEN}✓ Broker built successfully${NC}"

# Build Java SDK
echo -e "${BLUE}Building sdk-java (Maven)...${NC}"
cd sdk-java
mvn clean package -q
cd ..
echo -e "${GREEN}✓ Java SDK built successfully${NC}"

# Install Python SDK
echo -e "${BLUE}Installing analytics-py (Python)...${NC}"
cd analytics-py
pip install -e . -q
cd ..
echo -e "${GREEN}✓ Python SDK installed successfully${NC}"

echo -e "${GREEN}=== Build Complete ===${NC}"
echo ""
echo "To run the broker:"
echo "  ./broker/build/tvmjns-broker [port] [threads]"
echo ""
echo "To run the Java producer:"
echo "  cd sdk-java && mvn exec:java -Dexec.mainClass=\"com.tvmjns.sdk.ExampleProducer\""
echo ""
echo "To run the Python consumer:"
echo "  cd analytics-py && python example_consumer.py"
