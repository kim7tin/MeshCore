#pragma once

#include <stdint.h>

namespace mqttbridge {

// Source hashes of packets recently injected from MQTT. A direct ACK that reaches its last hop
// carries no destination, so it is sent to the topics of these hashes instead.
class ReplyCache {
  static const int SLOTS = 8;
  struct Entry {
    uint8_t hash;
    uint32_t noted;  // when it was last noted; age is compared as unsigned elapsed time
    bool used;
  };
  Entry _e[SLOTS];
  uint32_t _ttl_ms;
  int _next;

public:
  explicit ReplyCache(uint32_t ttl_ms = 15000UL) : _ttl_ms(ttl_ms), _next(0) {
    for (int i = 0; i < SLOTS; i++) _e[i].used = false;
  }

  void note(uint8_t hash, uint32_t now) {
    for (int i = 0; i < SLOTS; i++) {
      if (_e[i].used && _e[i].hash == hash) {
        _e[i].noted = now;
        return;
      }
    }
    _e[_next].hash = hash;
    _e[_next].noted = now;
    _e[_next].used = true;
    _next = (_next + 1) % SLOTS;
  }

  // Writes the unexpired hashes (unique, because note() refreshes in place) to 'out'.
  int collect(uint32_t now, uint8_t* out, int cap) const {
    int n = 0;
    for (int i = 0; i < SLOTS && n < cap; i++) {
      if (_e[i].used && (uint32_t)(now - _e[i].noted) < _ttl_ms) out[n++] = _e[i].hash;
    }
    return n;
  }

  // Frees expired entries. Call regularly: an entry nobody frees would look young again once
  // the millis() difference wraps around (after ~49 days).
  void prune(uint32_t now) {
    for (int i = 0; i < SLOTS; i++) {
      if (_e[i].used && (uint32_t)(now - _e[i].noted) >= _ttl_ms) _e[i].used = false;
    }
  }
};

}  // namespace mqttbridge
