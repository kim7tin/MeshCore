#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

namespace mqttbridge {

enum class TopicKind : uint8_t { NODE = 'n', GROUP = 'g' };

// Writes "<prefix>/<n|g>/<hh>" (hh = 2 lowercase hex chars) into out.
// Returns the length, or 0 if it does not fit (including the terminating NUL).
inline size_t makeTopic(const char* prefix, TopicKind kind, uint8_t hash, char* out, size_t cap) {
  int n = snprintf(out, cap, "%s/%c/%02x", prefix, (char)kind, (unsigned)hash);
  return (n > 0 && (size_t)n < cap) ? (size_t)n : 0;
}

inline int hexNibble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return -1;
}

// Inverse of makeTopic(). Rejects anything that makeTopic() would not have produced.
inline bool parseTopic(const char* prefix, const char* topic, TopicKind* kind, uint8_t* hash) {
  size_t plen = strlen(prefix);
  if (strncmp(topic, prefix, plen) != 0 || topic[plen] != '/') return false;
  const char* t = topic + plen + 1;  // "n/a7"
  if (strlen(t) != 4 || (t[0] != 'n' && t[0] != 'g') || t[1] != '/') return false;
  int hi = hexNibble(t[2]);
  int lo = hexNibble(t[3]);
  if (hi < 0 || lo < 0) return false;
  *kind = (TopicKind)t[0];
  *hash = (uint8_t)((hi << 4) | lo);
  return true;
}

}  // namespace mqttbridge
