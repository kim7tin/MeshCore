#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "BridgeCore.h"

namespace mqttbridge {

// Text for WiFi.status() (Arduino WL_* values).
inline const char* wifiStatusText(int s) {
  switch (s) {
    case 0: return "idle";
    case 1: return "network not found";
    case 3: return "connected";
    case 4: return "connect failed (wrong password?)";
    case 5: return "connection lost";
    case 6: return "disconnected";
    default: return "unknown";
  }
}

// Text for PubSubClient::state().
inline const char* mqttStateText(int s) {
  switch (s) {
    case -4: return "timeout";
    case -3: return "connection lost";
    case -2: return "cannot reach broker";
    case -1: return "disconnected";
    case 0: return "connected";
    case 1: return "protocol rejected";
    case 2: return "client id rejected";
    case 3: return "broker unavailable";
    case 4: return "bad user or password";
    case 5: return "not authorized";
    default: return "unknown";
  }
}

// One CLI reply line with every counter. Returns its length, or 0 if it does not fit in 'cap'.
inline int formatStats(const BridgeStats& s, char* out, size_t cap) {
  if (!out || cap == 0) return 0;
  // pub=published inj=injected flood/direct rate=dropped by rate limit q=dropped, queue full
  // dup=dropped duplicates sup=publishes suppressed own=own messages bad=bad envelope rej=rejected
  int n = snprintf(out, cap, "> pub %lu inj %luf/%lud rate %lu q %lu dup %lu sup %lu own %lu bad %lu rej %lu",
                   (unsigned long)s.published, (unsigned long)s.injected_flood,
                   (unsigned long)s.injected_direct, (unsigned long)s.dropped_ratelimit,
                   (unsigned long)s.dropped_queue_full, (unsigned long)s.dropped_duplicate,
                   (unsigned long)s.suppressed_publish, (unsigned long)s.own_dropped,
                   (unsigned long)s.bad_envelope, (unsigned long)s.rejected_policy);
  if (n < 0 || (size_t)n >= cap) {
    out[0] = 0;
    return 0;
  }
  return n;
}

}  // namespace mqttbridge
