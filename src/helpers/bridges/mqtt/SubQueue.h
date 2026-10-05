#pragma once

#include <stdint.h>

#include "MqttTopics.h"

namespace mqttbridge {

struct SubOp {
  bool subscribe;  // false = unsubscribe
  TopicKind kind;
  uint8_t hash;
};

// Subscribe/unsubscribe requests raised from the radio path. The adapter drains them from its
// loop() so that no network I/O happens while the radio is being serviced. If more than
// CAPACITY pile up, the oldest are kept, the rest are dropped and takeOverflow() tells the
// adapter to re-subscribe everything that is still active.
class SubQueue {
  static const int CAPACITY = 32;
  SubOp _q[CAPACITY];
  int _head, _count;
  bool _overflow;

public:
  SubQueue() : _head(0), _count(0), _overflow(false) {}

  void push(bool subscribe, TopicKind kind, uint8_t hash) {
    if (_count >= CAPACITY) {
      _overflow = true;
      return;
    }
    SubOp& op = _q[(_head + _count) % CAPACITY];
    op.subscribe = subscribe;
    op.kind = kind;
    op.hash = hash;
    _count++;
  }

  bool pop(SubOp* out) {
    if (_count == 0) return false;
    *out = _q[_head];
    _head = (_head + 1) % CAPACITY;
    _count--;
    return true;
  }

  // True once after an overflow happened.
  bool takeOverflow() {
    bool o = _overflow;
    _overflow = false;
    return o;
  }
};

}  // namespace mqttbridge
