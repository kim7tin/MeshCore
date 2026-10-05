#pragma once

#include <stdint.h>

namespace mqttbridge {

// Reconnect schedule: first attempt is immediate, then min, 2*min, ... capped at max.
class Backoff {
  uint32_t _min_ms, _max_ms, _cur_ms, _next_at;
  bool _waiting;

public:
  explicit Backoff(uint32_t min_ms = 1000, uint32_t max_ms = 60000)
      : _min_ms(min_ms), _max_ms(max_ms), _cur_ms(0), _next_at(0), _waiting(false) {}

  bool due(uint32_t now) const { return !_waiting || (int32_t)(now - _next_at) >= 0; }

  void fail(uint32_t now) {
    _cur_ms = (_cur_ms == 0) ? _min_ms : (_cur_ms * 2 > _max_ms ? _max_ms : _cur_ms * 2);
    _next_at = now + _cur_ms;
    _waiting = true;
  }

  void success() {
    _cur_ms = 0;
    _waiting = false;
  }
};

}  // namespace mqttbridge
