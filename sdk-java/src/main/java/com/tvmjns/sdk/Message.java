package com.tvmjns.sdk;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Binary protocol message representation.
 * Format: [magic:4][version:2][msg_type:2][payload_size:4][payload:N]
 * All fields are in little-endian byte order.
 */
public class Message {
    private static final int PROTOCOL_MAGIC = 0x544D564A; // "TVMJ"
    private static final short PROTOCOL_VERSION = 1;
    private static final int HEADER_SIZE = 12;

    public enum MessageType {
        PING(0),
        PONG(1),
        DATA(2),
        ACK(3),
        ERROR(4);

        private final short value;

        MessageType(int value) {
            this.value = (short) value;
        }

        public short getValue() {
            return value;
        }

        public static MessageType fromValue(short value) {
            for (MessageType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown message type: " + value);
        }
    }

    private final MessageType type;
    private final byte[] payload;

    public Message(MessageType type, byte[] payload) {
        this.type = type;
        this.payload = payload != null ? payload : new byte[0];
    }

    public Message(MessageType type) {
        this(type, null);
    }

    public MessageType getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getPayloadSize() {
        return payload.length;
    }

    /**
     * Serialize message to binary format (little-endian).
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Header
        buffer.putInt(PROTOCOL_MAGIC);
        buffer.putShort(PROTOCOL_VERSION);
        buffer.putShort(type.getValue());
        buffer.putInt(payload.length);

        // Payload
        if (payload.length > 0) {
            buffer.put(payload);
        }

        return buffer.array();
    }

    /**
     * Deserialize message from binary format (little-endian).
     */
    public static Message deserialize(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Data too short for header");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read header
        int magic = buffer.getInt();
        short version = buffer.getShort();
        short msgType = buffer.getShort();
        int payloadSize = buffer.getInt();

        // Validate
        if (magic != PROTOCOL_MAGIC) {
            throw new IllegalArgumentException(
                String.format("Invalid magic: 0x%08X", magic)
            );
        }
        if (version != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        if (payloadSize < 0 || payloadSize > data.length - HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid payload size: " + payloadSize);
        }

        // Read payload
        byte[] payload = new byte[payloadSize];
        if (payloadSize > 0) {
            buffer.get(payload);
        }

        return new Message(MessageType.fromValue(msgType), payload);
    }

    @Override
    public String toString() {
        return String.format("Message{type=%s, payload_size=%d}", type, payload.length);
    }
}
