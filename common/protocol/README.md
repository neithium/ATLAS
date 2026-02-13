# TVMJNS Binary Protocol Specification

## Overview
The TVMJNS protocol uses a simple binary format with a fixed-size header followed by a variable-length payload.

## Format (Little-Endian)

### Header (12 bytes)
```
+--------+--------+----------+--------------+
| magic  | version| msg_type | payload_size |
| 4 bytes| 2 bytes| 2 bytes  | 4 bytes      |
+--------+--------+----------+--------------+
```

- **magic** (4 bytes): Protocol identifier (0x544D564A = "TVMJ")
- **version** (2 bytes): Protocol version (currently 1)
- **msg_type** (2 bytes): Message type enum
- **payload_size** (4 bytes): Size of payload in bytes

### Message Types
- 0: PING - Health check request
- 1: PONG - Health check response
- 2: DATA - Application data
- 3: ACK - Acknowledgment
- 4: ERROR - Error message

### Payload (variable length)
Raw binary data, interpreted based on msg_type.

## Endianness
All multi-byte fields use little-endian byte order.

## Example
```
DATA message with 100-byte payload:
Header: 4A 56 4D 54 01 00 02 00 64 00 00 00
Payload: [100 bytes of data]
```
