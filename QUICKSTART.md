# TVMJNS Quick Start Guide

This guide will help you get TVMJNS up and running in 5 minutes.

## Prerequisites

Choose one of the following:

### Option 1: Native Build
- Linux with epoll support
- C++17 compiler (GCC 7+)
- CMake 3.10+
- Java 11+ and Maven 3.6+
- Python 3.7+

### Option 2: Docker
- Docker 20.10+
- Docker Compose V2

## Quick Start (Native)

### 1. Build All Components

```bash
./build.sh
```

This will build:
- C++ broker
- Java SDK
- Python SDK (installed in development mode)

### 2. Start the Broker

```bash
./broker/build/tvmjns-broker 9090 4
```

Output:
```
[2026-02-13 07:15:00] [INFO] TVMJNS Broker starting...
[2026-02-13 07:15:00] [INFO] TcpServer created on port 9090 with 4 threads
[2026-02-13 07:15:00] [INFO] TcpServer started on port 9090
```

### 3. Run Java Producer (in another terminal)

```bash
cd sdk-java
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"
```

Output:
```
[main] INFO com.tvmjns.sdk.ExampleProducer - Starting TVMJNS Example Producer
[main] INFO com.tvmjns.sdk.TvmjnsClient - Connected to localhost:9090
[main] INFO com.tvmjns.sdk.ExampleProducer - Ping successful
[main] INFO com.tvmjns.sdk.ExampleProducer - Sent: Message 0: Hello from Java!
...
```

### 4. Run Python Consumer (in another terminal)

```bash
cd analytics-py
python example_consumer.py
```

Output:
```
[2026-02-13 07:15:05] [INFO] Starting TVMJNS Example Consumer
[2026-02-13 07:15:05] [INFO] Connected to broker at localhost:9090
[2026-02-13 07:15:05] [INFO] Ping successful
```

## Quick Start (Docker)

### 1. Build and Start with Docker Compose

```bash
cd deploy
docker compose up --build
```

The broker will be available on `localhost:9090`.

### 2. Test with Java Producer

```bash
cd sdk-java
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"
```

### 3. Test with Python Consumer

```bash
cd analytics-py
python example_consumer.py
```

## Verify Installation

### Test Broker Connection

```bash
# Using netcat
echo | nc localhost 9090 && echo "Broker is listening"

# Using telnet
telnet localhost 9090
```

### Test End-to-End

```bash
./test-integration.sh
```

Expected output:
```
=== TVMJNS Integration Test ===

✓ Broker is running
✓ Java producer connected successfully
✓ Python consumer connected and ping successful

=== Integration Test Complete ===
```

## Next Steps

1. **Read the Documentation**:
   - [Binary Protocol](common/protocol/README.md)
   - [Broker Architecture](broker/README.md)
   - [Java SDK](sdk-java/README.md)
   - [Python SDK](analytics-py/README.md)

2. **Customize Your Application**:
   - Modify `ExampleProducer.java` for your producer logic
   - Modify `example_consumer.py` for your consumer logic
   - Extend the binary protocol if needed

3. **Deploy to Production**:
   - See [deploy/README.md](deploy/README.md) for production deployment
   - Configure monitoring and logging
   - Set up load balancing

## Troubleshooting

### Broker won't start

- Check if port 9090 is already in use: `lsof -i :9090`
- Check firewall settings
- Verify build completed successfully

### Connection refused

- Ensure broker is running: `ps aux | grep tvmjns-broker`
- Check broker logs for errors
- Verify network connectivity

### Build errors

- Ensure all dependencies are installed
- Check compiler version: `g++ --version` (need 7+)
- Clean and rebuild: `./clean.sh && ./build.sh`

## Configuration

### Broker Port and Threads

```bash
./broker/build/tvmjns-broker <port> <num_threads>
```

Example: Run on port 8080 with 8 threads:
```bash
./broker/build/tvmjns-broker 8080 8
```

### Java Client Configuration

```java
TvmjnsClient client = new TvmjnsClient("hostname", port);
```

### Python Client Configuration

```python
consumer = TvmjnsConsumer('hostname', port)
```

## Performance Tips

1. **Increase Thread Pool**: For high concurrency, increase thread count
2. **Tune TCP Settings**: Adjust socket buffer sizes
3. **Use Binary Payloads**: Avoid string encoding overhead
4. **Connection Pooling**: Reuse connections when possible
5. **Batch Processing**: Send/receive messages in batches

## Support

For issues or questions:
1. Check the component-specific README files
2. Review the binary protocol specification
3. Check system logs for errors
4. Verify network connectivity and firewall rules
