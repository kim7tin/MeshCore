#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

namespace mqttbridge {

// Wire format of every MQTT message: [ver 1B][flags 1B][relay_id 4B][raw packet bytes]
// where the raw packet is exactly what mesh::Packet::writeTo() produces.
constexpr uint8_t ENVELOPE_VERSION = 1;
constexpr size_t ENVELOPE_HEADER_LEN = 6;

inline size_t encodeEnvelope(const uint8_t relay_id[4], const uint8_t* raw, size_t raw_len,
                             uint8_t* out, size_t cap) {
  if (raw_len == 0 || cap < ENVELOPE_HEADER_LEN + raw_len) return 0;
  out[0] = ENVELOPE_VERSION;
  out[1] = 0;  // flags, reserved
  memcpy(&out[2], relay_id, 4);
  memcpy(&out[ENVELOPE_HEADER_LEN], raw, raw_len);
  return ENVELOPE_HEADER_LEN + raw_len;
}

// On success 'raw' points into 'in' (no copy) and raw_len >= 1.
inline bool decodeEnvelope(const uint8_t* in, size_t len, uint8_t relay_id[4], const uint8_t** raw,
                           size_t* raw_len) {
  if (len <= ENVELOPE_HEADER_LEN || in[0] != ENVELOPE_VERSION) return false;
  memcpy(relay_id, &in[2], 4);
  *raw = &in[ENVELOPE_HEADER_LEN];
  *raw_len = len - ENVELOPE_HEADER_LEN;
  return true;
}

}  // namespace mqttbridge
