# TVMJNS - Transactional Velocity Messaging for Joint Networked Systems

A high-performance, production-ready distributed messaging system with a modular monorepo architecture.

## Architecture

TVMJNS consists of five main components:

1. **broker**: High-performance C++17 message broker with epoll, thread pool, and non-blocking I/O
2. **sdk-java**: Maven-based Java SDK for message producers
3. **analytics-py**: Python SDK for message consumers
4. **common**: Binary protocol specification (little-endian)
5. **deploy**: Docker and Docker Compose deployment configurations

## Features

- ⚡ **High Performance**: C++17 with epoll, thread pool, and non-blocking I/O
- 🔒 **Thread-Safe**: Concurrent connection handling with proper synchronization
- 📦 **Binary Protocol**: Efficient little-endian binary format
- 🌐 **Cross-Platform**: Java producers, Python consumers, C++ broker
- 🐳 **Docker Ready**: Complete containerization with health checks
- 📊 **Production Ready**: Logging, error handling, and modular architecture

## Binary Protocol

The TVMJNS protocol uses a simple binary format:

```
Header (12 bytes, Little-Endian):
  [magic:4] [version:2] [msg_type:2] [payload_size:4]

Payload (variable):
  [data:payload_size]
```

Message Types:
- `PING (0)`: Health check request
- `PONG (1)`: Health check response
- `DATA (2)`: Application data
- `ACK (3)`: Acknowledgment
- `ERROR (4)`: Error message

See [common/protocol/README.md](common/protocol/README.md) for details.

## Quick Start

### Build and Run Broker

```bash
cd broker
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
./tvmjns-broker 9090 4
```

### Java Producer

```bash
cd sdk-java
mvn clean package
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"
```

### Python Consumer

```bash
cd analytics-py
pip install -e .
python example_consumer.py localhost 9090
```

### Docker Deployment

```bash
cd deploy
docker-compose up --build
```

## Components

### Broker (C++17)

High-performance message broker featuring:
- **TcpServer**: Accepts concurrent connections using epoll
- **ThreadPool**: Manages worker threads for connection handling
- **Non-blocking I/O**: Edge-triggered epoll for maximum throughput
- **Thread-safe**: Proper synchronization for concurrent access
- **Logging**: Comprehensive logging infrastructure

Dependencies:
- C++17 compiler (GCC 7+, Clang 5+)
- CMake 3.10+
- Linux with epoll support

### SDK-Java (Maven)

Java client library for message producers:
- **TvmjnsClient**: Thread-safe client for sending messages
- **Message**: Binary protocol serialization/deserialization
- **TCP Sockets**: Java NIO for network communication
- **Zero-copy hints**: Efficient binary struct packing

Dependencies:
- Java 11+
- Maven 3.6+
- SLF4J for logging

### Analytics-Py (Python)

Python client library for message consumers:
- **TvmjnsConsumer**: Consumer client for receiving messages
- **Message**: Binary protocol handling with struct module
- **Context Manager**: Pythonic resource management

Dependencies:
- Python 3.7+
- No external dependencies (stdlib only)

## Performance Considerations

1. **Zero-Copy**: Use scatter-gather I/O (writev) for efficient data transfer
2. **Thread Pool**: Configurable size based on workload
3. **Non-blocking I/O**: Edge-triggered epoll for high concurrency
4. **TCP_NODELAY**: Disabled Nagle's algorithm for low latency
5. **Binary Protocol**: Minimal overhead with fixed-size headers

## Development

### Building from Source

```bash
# Build broker
cd broker
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make

# Build Java SDK
cd sdk-java
mvn clean install

# Install Python SDK
cd analytics-py
pip install -e .
```

### Running Tests

```bash
# Start broker
./broker/build/tvmjns-broker 9090 4

# Run Java producer
cd sdk-java
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer" -Dexec.args="localhost 9090"

# Run Python consumer
cd analytics-py
python example_consumer.py localhost 9090
```

## Directory Structure

```
TVMJNS/
├── broker/              # C++17 message broker
│   ├── include/         # Header files
│   ├── src/             # Source files
│   └── CMakeLists.txt   # CMake configuration
├── sdk-java/            # Java SDK for producers
│   ├── src/             # Java source code
│   └── pom.xml          # Maven configuration
├── analytics-py/        # Python SDK for consumers
│   ├── tvmjns/          # Python package
│   ├── example_consumer.py
│   └── setup.py         # Package configuration
├── common/              # Shared protocol specification
│   └── protocol/        # Binary protocol headers
├── deploy/              # Deployment configurations
│   ├── Dockerfile       # Broker container image
│   └── docker-compose.yml
└── README.md            # This file
```

## Production Deployment

1. **Load Balancing**: Deploy multiple broker instances behind a load balancer
2. **Monitoring**: Integrate with Prometheus/Grafana for metrics
3. **Logging**: Centralize logs with ELK stack or similar
4. **Security**: Add TLS/SSL for encrypted communication
5. **Persistence**: Implement message persistence if required

## License

See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style conventions
- All tests pass
- Documentation is updated
- Commits are atomic and well-described
