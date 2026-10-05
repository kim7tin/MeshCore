#pragma once

#include <stdint.h>
#include <string.h>

namespace mqttbridge {

// The relay id in every MQTT message (and the MQTT client id) must differ between relays.
// Derive it from the node's public key: random, and already unique per repeater. Byte 0 is the
// node hash that is visible on the air, so skip it. (Do not use the chip MAC prefix: boards from
// one batch share its first bytes.)
inline void relayIdFromPubKey(const uint8_t* pub_key, uint8_t out[4]) { memcpy(out, pub_key + 1, 4); }

}  // namespace mqttbridge
