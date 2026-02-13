# TVMJNS Analytics Python SDK

Python consumer client for TVMJNS message broker.

## Installation

```bash
pip install -e .
```

## Usage

```python
from tvmjns import TvmjnsConsumer, MessageType

# Create consumer
consumer = TvmjnsConsumer('localhost', 9090)
consumer.connect()

# Test connection
consumer.ping()

# Receive messages
while True:
    message = consumer.receive()
    if message and message.msg_type == MessageType.DATA:
        print(f"Received: {message.payload.decode('utf-8')}")

consumer.disconnect()
```

## Example

Run the example consumer:

```bash
python example_consumer.py [host] [port]
```

Default: `localhost:9090`
