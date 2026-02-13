# TVMJNS Java SDK

Maven-based Java SDK for TVMJNS message broker. Thread-safe producer client for sending binary messages.

## Features

- Thread-safe client implementation
- Binary protocol serialization/deserialization
- TCP socket communication with non-blocking hints
- Connection pooling ready
- SLF4J logging integration

## Installation

### Maven Dependency

```xml
<dependency>
    <groupId>com.tvmjns</groupId>
    <artifactId>sdk-java</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Build from Source

```bash
mvn clean install
```

## Usage

### Basic Producer

```java
import com.tvmjns.sdk.TvmjnsClient;
import com.tvmjns.sdk.Message;

public class MyProducer {
    public static void main(String[] args) throws Exception {
        // Create and connect client
        try (TvmjnsClient client = new TvmjnsClient("localhost", 9090)) {
            client.connect();
            
            // Test connection
            if (client.ping()) {
                System.out.println("Connected!");
            }
            
            // Send string data
            client.sendData("Hello, TVMJNS!");
            
            // Send binary data
            byte[] data = new byte[]{1, 2, 3, 4, 5};
            client.sendData(data);
        }
    }
}
```

### Advanced Usage

```java
// Create custom message
Message message = new Message(
    Message.MessageType.DATA,
    "Custom payload".getBytes(StandardCharsets.UTF_8)
);
client.send(message);

// Check connection status
if (client.isConnected()) {
    // Send data
}

// Manual connection management
TvmjnsClient client = new TvmjnsClient("localhost", 9090);
client.connect();
// ... use client ...
client.disconnect();
```

## API Reference

### TvmjnsClient

Main client class for connecting to the broker.

**Constructor:**
```java
TvmjnsClient(String host, int port)
```

**Methods:**
- `void connect()` - Connect to the broker
- `void disconnect()` - Disconnect from the broker
- `void send(Message message)` - Send a message
- `void sendData(String data)` - Send string data
- `void sendData(byte[] data)` - Send binary data
- `boolean ping()` - Test connection with ping/pong
- `boolean isConnected()` - Check connection status

### Message

Binary protocol message representation.

**Constructor:**
```java
Message(MessageType type, byte[] payload)
Message(MessageType type)  // Empty payload
```

**Methods:**
- `byte[] serialize()` - Serialize to binary format
- `static Message deserialize(byte[] data)` - Deserialize from binary
- `MessageType getType()` - Get message type
- `byte[] getPayload()` - Get payload data
- `int getPayloadSize()` - Get payload size

**MessageType Enum:**
- `PING` - Health check request
- `PONG` - Health check response
- `DATA` - Application data
- `ACK` - Acknowledgment
- `ERROR` - Error message

## Thread Safety

`TvmjnsClient` is thread-safe. Multiple threads can safely call `send()` methods concurrently. The class uses internal synchronization to ensure thread safety.

## Performance Tips

1. **Connection Reuse**: Keep connections open and reuse them
2. **Batch Sending**: Send multiple messages in a loop
3. **Binary Payloads**: Use byte arrays for better performance
4. **Connection Pooling**: Implement connection pool for high-throughput scenarios

## Example Application

Run the example producer:

```bash
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer" -Dexec.args="localhost 9090"
```

## Dependencies

- Java 11+
- Maven 3.6+
- SLF4J 1.7+

## Building

```bash
# Compile
mvn compile

# Package
mvn package

# Install to local Maven repository
mvn install

# Run example
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"
```

## Testing

```bash
# Make sure broker is running
./broker/build/tvmjns-broker 9090 4

# Run example producer
mvn exec:java -Dexec.mainClass="com.tvmjns.sdk.ExampleProducer"
```

## Protocol

See [../common/protocol/README.md](../common/protocol/README.md) for binary protocol specification.
