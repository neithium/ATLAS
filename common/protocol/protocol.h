#ifndef TVMJNS_PROTOCOL_H
#define TVMJNS_PROTOCOL_H

#include <cstdint>
#include <cstring>

namespace tvmjns {
namespace protocol {

// Binary Protocol Format (Little-Endian)
// Header: [magic:4][version:2][msg_type:2][payload_size:4]
// Payload: [data:payload_size]

constexpr uint32_t PROTOCOL_MAGIC = 0x544D564A; // "TVMJ" in hex
constexpr uint16_t PROTOCOL_VERSION = 1;

enum class MessageType : uint16_t {
    PING = 0,
    PONG = 1,
    DATA = 2,
    ACK = 3,
    ERROR = 4
};

#pragma pack(push, 1)
struct MessageHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t msg_type;
    uint32_t payload_size;

    MessageHeader() 
        : magic(PROTOCOL_MAGIC)
        , version(PROTOCOL_VERSION)
        , msg_type(0)
        , payload_size(0) {}

    MessageHeader(MessageType type, uint32_t size)
        : magic(PROTOCOL_MAGIC)
        , version(PROTOCOL_VERSION)
        , msg_type(static_cast<uint16_t>(type))
        , payload_size(size) {}

    bool isValid() const {
        return magic == PROTOCOL_MAGIC && version == PROTOCOL_VERSION;
    }
};
#pragma pack(pop)

static_assert(sizeof(MessageHeader) == 12, "MessageHeader must be 12 bytes");

} // namespace protocol
} // namespace tvmjns

#endif // TVMJNS_PROTOCOL_H
