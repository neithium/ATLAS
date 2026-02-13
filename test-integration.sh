#!/bin/bash
# Integration test for TVMJNS

set -e

echo "=== TVMJNS Integration Test ==="
echo ""

# Check if broker is running
if ! pgrep -f tvmjns-broker > /dev/null; then
    echo "ERROR: Broker is not running"
    exit 1
fi
echo "✓ Broker is running"

# Test Java producer
echo ""
echo "Testing Java producer..."
cd sdk-java
timeout 15 mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer" -Dexec.args="localhost 9090" -q > /tmp/java-producer.log 2>&1 || true
if grep -q "Connected to broker" /tmp/java-producer.log; then
    echo "✓ Java producer connected successfully"
else
    echo "✗ Java producer failed to connect"
    cat /tmp/java-producer.log
fi
cd ..

# Test Python consumer (just connection test)
echo ""
echo "Testing Python consumer..."
cd analytics-py
timeout 5 python3 << 'EOF' > /tmp/python-consumer.log 2>&1 || true
from tvmjns import TvmjnsConsumer
consumer = TvmjnsConsumer('localhost', 9090)
consumer.connect()
if consumer.ping():
    print("✓ Python consumer connected and ping successful")
else:
    print("✗ Python consumer ping failed")
consumer.disconnect()
EOF
cat /tmp/python-consumer.log
cd ..

echo ""
echo "=== Integration Test Complete ==="
