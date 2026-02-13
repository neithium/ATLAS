# TVMJNS Broker - C++17 Message Broker

High-performance message broker with epoll, thread pool, and non-blocking I/O.

## Features

- **C++17**: Modern C++ with standard library features
- **Epoll**: Edge-triggered epoll for efficient event handling
- **ThreadPool**: Configurable thread pool for concurrent connection handling
- **Non-blocking I/O**: Zero-copy hints with scatter-gather I/O
- **Thread-safe**: Proper synchronization primitives
- **Logging**: Comprehensive logging infrastructure

## Architecture

```
broker/
├── include/           # Header files
│   ├── tcp_server.h  # TcpServer class with epoll
│   ├── thread_pool.h # ThreadPool implementation
│   └── logger.h      # Logging utilities
├── src/              # Implementation files
│   ├── main.cpp      # Entry point
│   ├── tcp_server.cpp
│   ├── thread_pool.cpp
│   └── logger.cpp
└── CMakeLists.txt    # Build configuration
```

## Building

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

For debug build:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug ..
```

## Running

```bash
./tvmjns-broker [port] [num_threads]
```

Default: `port=9090`, `num_threads=4`

Example:
```bash
./tvmjns-broker 8080 8
```

## Components

### TcpServer

Manages TCP connections using epoll and non-blocking I/O:

- Creates listening socket on specified port
- Uses edge-triggered epoll for event notification
- Accepts connections in non-blocking mode
- Delegates connection handling to thread pool
- Implements binary protocol parsing

### ThreadPool

Worker thread pool for concurrent connection handling:

- Configurable number of worker threads
- Thread-safe task queue
- Graceful shutdown support
- Exception-safe task execution

### Logger

Simple but effective logging system:

- Multiple log levels (DEBUG, INFO, WARN, ERROR)
- Thread-safe logging
- Timestamp and level prefix
- Configurable log level at runtime

## Performance Tuning

1. **Thread Pool Size**: Set based on CPU cores and workload
   - CPU-bound: num_cores
   - I/O-bound: num_cores * 2 or more

2. **Socket Options**:
   - TCP_NODELAY: Enabled for low latency
   - SO_REUSEADDR: Enabled for quick restart

3. **Epoll Mode**: Edge-triggered for efficiency

4. **Zero-Copy**: Consider using sendfile() or splice() for large payloads

## Dependencies

- Linux with epoll support (kernel 2.6+)
- C++17 compiler (GCC 7+, Clang 5+)
- CMake 3.10+
- pthread

## Protocol

See [../common/protocol/README.md](../common/protocol/README.md) for binary protocol specification.

## Testing

Start the broker and use Java or Python clients to test:

```bash
# Terminal 1: Start broker
./tvmjns-broker 9090 4

# Terminal 2: Run Java producer
cd ../sdk-java
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"

# Terminal 3: Run Python consumer
cd ../analytics-py
python example_consumer.py
```

## Production Considerations

1. **Resource Limits**: Configure ulimit for file descriptors
2. **Monitoring**: Add metrics collection (connections, throughput, latency)
3. **Graceful Shutdown**: Handle SIGTERM for clean shutdown
4. **Logging**: Redirect to file or syslog
5. **Security**: Add authentication and TLS support
