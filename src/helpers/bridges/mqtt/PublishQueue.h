#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "MqttTopics.h"

namespace mqttbridge {

struct PendingPublish {
  bool used;
  TopicKind kind;
  uint8_t hash;
  uint8_t key[8];   // packet key, used to cancel when the same packet shows up on MQTT first
  uint32_t due;
  uint8_t len;
  uint8_t data[255];  // raw packet (Packet::writeTo output)
};

// Holds publishes during their random 0..jitter delay so they can be cancelled.
class PublishQueue {
  static const int SLOTS = 16;
  PendingPublish _q[SLOTS];

public:
  PublishQueue() {
    for (int i = 0; i < SLOTS; i++) _q[i].used = false;
  }

  bool push(TopicKind kind, uint8_t hash, const uint8_t key[8], const uint8_t* raw, size_t len,
            uint32_t due) {
    if (len == 0 || len > 255) return false;
    for (int i = 0; i < SLOTS; i++) {
      if (!_q[i].used) {
        _q[i].used = true;
        _q[i].kind = kind;
        _q[i].hash = hash;
        memcpy(_q[i].key, key, 8);
        _q[i].due = due;
        _q[i].len = (uint8_t)len;
        memcpy(_q[i].data, raw, len);
        return true;
      }
    }
    return false;
  }

  // Removes every entry with this key. Returns how many were removed.
  int cancel(const uint8_t key[8]) {
    int n = 0;
    for (int i = 0; i < SLOTS; i++) {
      if (_q[i].used && memcmp(_q[i].key, key, 8) == 0) {
        _q[i].used = false;
        n++;
      }
    }
    return n;
  }

  // Moves one due entry into 'out' and frees its slot.
  bool popDue(uint32_t now, PendingPublish* out) {
    for (int i = 0; i < SLOTS; i++) {
      if (_q[i].used && (int32_t)(now - _q[i].due) >= 0) {
        *out = _q[i];
        _q[i].used = false;
        return true;
      }
    }
    return false;
  }
};

}  // namespace mqttbridge
