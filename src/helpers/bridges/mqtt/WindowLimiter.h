#pragma once

#include <stdint.h>

namespace mqttbridge {

// Fixed-window limiter: at most 'max' events per 'window_ms'. max == 0 blocks everything.
class WindowLimiter {
  uint16_t _limit;  // not "_max": the ESP32 core defines a _max() macro
  uint32_t _window_ms;
  uint32_t _start;
  uint16_t _count;
  bool _started;

public:
  WindowLimiter(uint16_t max, uint32_t window_ms)
      : _limit(max), _window_ms(window_ms), _start(0), _count(0), _started(false) {}

  bool allow(uint32_t now) {
    if (!_started || (uint32_t)(now - _start) >= _window_ms) {
      _started = true;
      _start = now;
      _count = 0;
    }
    if (_count >= _limit) return false;
    _count++;
    return true;
  }
};

}  // namespace mqttbridge
