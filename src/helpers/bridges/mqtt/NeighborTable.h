#pragma once

#include <stdint.h>
#include <string.h>

namespace mqttbridge {

// Tracks which 1-byte node hashes were heard recently. The caller subscribes to a hash's
// topic when touch() says it is new, and unsubscribes when expire() reports it.
class NeighborTable {
  uint32_t _expiry[256];
  bool _active[256];
  uint32_t _ttl_ms;

public:
  explicit NeighborTable(uint32_t ttl_ms = 180000UL) : _ttl_ms(ttl_ms) {
    memset(_expiry, 0, sizeof(_expiry));
    memset(_active, 0, sizeof(_active));
  }

  // Marks 'hash' as heard at 'now'. Returns true if it was not active before.
  bool touch(uint8_t hash, uint32_t now) {
    bool fresh = !_active[hash];
    _active[hash] = true;
    _expiry[hash] = now + _ttl_ms;
    return fresh;
  }

  bool isActive(uint8_t hash) const { return _active[hash]; }

  // Deactivates entries whose time is up and writes them to 'out' (at most 'cap').
  // Entries beyond 'cap' stay active and are reported by the next call.
  // Uses wrap-safe signed difference, so it is correct across millis() overflow.
  int expire(uint32_t now, uint8_t* out, int cap) {
    int n = 0;
    for (int h = 0; h < 256 && n < cap; h++) {
      if (_active[h] && (int32_t)(now - _expiry[h]) >= 0) {
        _active[h] = false;
        out[n++] = (uint8_t)h;
      }
    }
    return n;
  }

  int collectActive(uint8_t* out, int cap) const {
    int n = 0;
    for (int h = 0; h < 256 && n < cap; h++) {
      if (_active[h]) out[n++] = (uint8_t)h;
    }
    return n;
  }
};

}  // namespace mqttbridge
