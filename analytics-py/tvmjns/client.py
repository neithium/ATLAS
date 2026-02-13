"""
TVMJNS Python Analytics SDK - Consumer Client
"""

import socket
import struct
import logging
from enum import IntEnum
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class MessageType(IntEnum):
    """Message types matching the protocol specification."""
    PING = 0
    PONG = 1
    DATA = 2
    ACK = 3
    ERROR = 4


class Message:
    """
    Binary protocol message.
    Format (Little-Endian):
        Header: [magic:4][version:2][msg_type:2][payload_size:4]
        Payload: [data:payload_size]
    """
    PROTOCOL_MAGIC = 0x544D564A  # "TVMJ"
    PROTOCOL_VERSION = 1
    HEADER_SIZE = 12
    HEADER_FORMAT = '<IHHI'  # Little-endian: uint32, uint16, uint16, uint32

    def __init__(self, msg_type: MessageType, payload: bytes = b''):
        self.msg_type = msg_type
        self.payload = payload

    def serialize(self) -> bytes:
        """Serialize message to binary format."""
        header = struct.pack(
            self.HEADER_FORMAT,
            self.PROTOCOL_MAGIC,
            self.PROTOCOL_VERSION,
            self.msg_type,
            len(self.payload)
        )
        return header + self.payload

    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize message from binary format."""
        if len(data) < cls.HEADER_SIZE:
            raise ValueError(f"Data too short: {len(data)} < {cls.HEADER_SIZE}")

        # Unpack header
        magic, version, msg_type, payload_size = struct.unpack(
            cls.HEADER_FORMAT,
            data[:cls.HEADER_SIZE]
        )

        # Validate
        if magic != cls.PROTOCOL_MAGIC:
            raise ValueError(f"Invalid magic: 0x{magic:08X}")
        if version != cls.PROTOCOL_VERSION:
            raise ValueError(f"Unsupported version: {version}")

        # Extract payload
        payload = data[cls.HEADER_SIZE:cls.HEADER_SIZE + payload_size]
        if len(payload) != payload_size:
            raise ValueError(f"Incomplete payload: {len(payload)} != {payload_size}")

        return cls(MessageType(msg_type), payload)

    def __repr__(self):
        return f"Message(type={self.msg_type.name}, payload_size={len(self.payload)})"


class TvmjnsConsumer:
    """
    Python consumer client for TVMJNS broker.
    Thread-safe consumer for receiving and processing messages.
    """

    def __init__(self, host: str = 'localhost', port: int = 9090):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.connected = False

    def connect(self):
        """Connect to the broker."""
        if self.connected:
            logger.warning("Already connected")
            return

        logger.info(f"Connecting to {self.host}:{self.port}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket.settimeout(5.0)
        self.socket.connect((self.host, self.port))
        self.connected = True
        logger.info(f"Connected to {self.host}:{self.port}")

    def disconnect(self):
        """Disconnect from the broker."""
        if not self.connected:
            return

        logger.info(f"Disconnecting from {self.host}:{self.port}")
        if self.socket:
            self.socket.close()
            self.socket = None
        self.connected = False
        logger.info("Disconnected")

    def send(self, message: Message):
        """Send a message to the broker."""
        if not self.connected:
            raise RuntimeError("Not connected")

        data = message.serialize()
        self.socket.sendall(data)
        logger.debug(f"Sent: {message}")

    def receive(self) -> Optional[Message]:
        """Receive a message from the broker."""
        if not self.connected:
            raise RuntimeError("Not connected")

        try:
            # Read header
            header_data = self._recv_exactly(Message.HEADER_SIZE)
            if not header_data:
                return None

            # Parse header to get payload size
            magic, version, msg_type, payload_size = struct.unpack(
                Message.HEADER_FORMAT,
                header_data
            )

            # Read payload
            payload_data = b''
            if payload_size > 0:
                payload_data = self._recv_exactly(payload_size)
                if not payload_data:
                    return None

            # Deserialize complete message
            full_data = header_data + payload_data
            message = Message.deserialize(full_data)
            logger.debug(f"Received: {message}")
            return message

        except socket.timeout:
            logger.debug("Receive timeout")
            return None
        except Exception as e:
            logger.error(f"Receive error: {e}")
            return None

    def _recv_exactly(self, n: int) -> Optional[bytes]:
        """Receive exactly n bytes from the socket."""
        data = b''
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                logger.warning("Connection closed")
                return None
            data += chunk
        return data

    def ping(self) -> bool:
        """Send ping and wait for pong."""
        try:
            ping_msg = Message(MessageType.PING)
            self.send(ping_msg)
            
            response = self.receive()
            if response and response.msg_type == MessageType.PONG:
                logger.debug("Ping successful")
                return True
            return False
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            return False

    def send_data(self, data: bytes):
        """Send a data message."""
        message = Message(MessageType.DATA, data)
        self.send(message)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
