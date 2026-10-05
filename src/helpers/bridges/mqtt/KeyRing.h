#pragma once

#include <stdint.h>
#include <string.h>

namespace mqttbridge {

// Remembers the last 32 packet keys (8-byte packet hashes). Oldest entry is overwritten first.
class KeyRing {
  static const int SLOTS = 32;
  uint8_t _key[SLOTS][8];
  bool _used[SLOTS];
  int _next;

public:
  KeyRing() : _next(0) { memset(_used, 0, sizeof(_used)); }

  bool contains(const uint8_t key[8]) const {
    for (int i = 0; i < SLOTS; i++) {
      if (_used[i] && memcmp(_key[i], key, 8) == 0) return true;
    }
    return false;
  }

  void add(const uint8_t key[8]) {
    if (contains(key)) return;
    memcpy(_key[_next], key, 8);
    _used[_next] = true;
    _next = (_next + 1) % SLOTS;
  }
};

}  // namespace mqttbridge
