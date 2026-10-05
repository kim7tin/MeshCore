#pragma once

#include <stdint.h>

namespace mqttbridge {

// Fixed-window limiter: at most 'max' events per 'window_ms'. max == 0 blocks everything.
class WindowLimiter {
  uint16_t _max;
  uint32_t _window_ms;
  uint32_t _start;
  uint16_t _count;
  bool _started;

public:
  WindowLimiter(uint16_t max, uint32_t window_ms)
      : _max(max), _window_ms(window_ms), _start(0), _count(0), _started(false) {}

  bool allow(uint32_t now) {
    if (!_started || (uint32_t)(now - _start) >= _window_ms) {
      _started = true;
      _start = now;
      _count = 0;
    }
    if (_count >= _max) return false;
    _count++;
    return true;
  }
};

}  // namespace mqttbridge
