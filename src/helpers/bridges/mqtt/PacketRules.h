#pragma once

#include <Packet.h>
#include <stddef.h>
#include <stdint.h>

namespace mqttbridge {

// 0x00 and 0xFF are reserved node hashes in MeshCore; never turn them into topics.
inline bool isReservedHash(uint8_t h) { return h == 0x00 || h == 0xFF; }

// Hash of the packet's original sender, when the payload carries one.
inline bool srcHash(const mesh::Packet& p, uint8_t* h) {
  switch (p.getPayloadType()) {
    case PAYLOAD_TYPE_TXT_MSG:
    case PAYLOAD_TYPE_REQ:
    case PAYLOAD_TYPE_RESPONSE:
    case PAYLOAD_TYPE_PATH:  // payload: dest_hash, src_hash, MAC, data
      if (p.payload_len < 2) return false;
      *h = p.payload[1];
      return true;
    case PAYLOAD_TYPE_ANON_REQ:  // payload: dest_hash, sender public key (32), MAC, data
      if (p.payload_len < 1 + PUB_KEY_SIZE) return false;
      *h = p.payload[1];
      return true;
    case PAYLOAD_TYPE_ADVERT:  // payload: public key (32), timestamp, signature, app data
      if (p.payload_len < PUB_KEY_SIZE) return false;
      *h = p.payload[0];
      return true;
    default:
      return false;
  }
}

// Hash of the destination node, when the payload carries one.
inline bool destHash(const mesh::Packet& p, uint8_t* h) {
  switch (p.getPayloadType()) {
    case PAYLOAD_TYPE_TXT_MSG:
    case PAYLOAD_TYPE_REQ:
    case PAYLOAD_TYPE_RESPONSE:
    case PAYLOAD_TYPE_PATH:
    case PAYLOAD_TYPE_ANON_REQ:
      if (p.payload_len < 1) return false;
      *h = p.payload[0];
      return true;
    default:
      return false;
  }
}

// The node we just heard transmitting this packet, as far as the packet reveals it.
// Flood only: the last hop in the path, or (path empty) the original sender.
// For multi-byte path hashes the topic uses the first byte of that hash.
inline bool nearbyHash(const mesh::Packet& p, uint8_t* h) {
  if (!p.isRouteFlood()) return false;
  uint8_t n = p.getPathHashCount();
  uint8_t found;
  if (n > 0) {
    found = p.path[(n - 1) * p.getPathHashSize()];
  } else if (!srcHash(p, &found)) {
    return false;
  }
  if (isReservedHash(found)) return false;
  *h = found;
  return true;
}

// Bounds check for untrusted raw packets (from MQTT). mesh::Packet::readFrom() reads the header,
// transport codes and path before looking at 'len', so call this first.
inline bool isWellFormedRaw(const uint8_t* raw, size_t len) {
  if (len < 2) return false;
  uint8_t route = raw[0] & PH_ROUTE_MASK;
  bool transport = route == ROUTE_TYPE_TRANSPORT_FLOOD || route == ROUTE_TYPE_TRANSPORT_DIRECT;
  size_t i = 1 + (transport ? 4 : 0);
  if (i >= len) return false;  // no path_len byte
  uint8_t path_len = raw[i++];
  if (!mesh::Packet::isValidPathLen(path_len)) return false;
  size_t path_bytes = (size_t)(path_len & 63) * (size_t)((path_len >> 6) + 1);
  if (i + path_bytes >= len) return false;  // needs at least one payload byte
  return len - i - path_bytes <= MAX_PACKET_PAYLOAD;
}

inline bool isBridgeableDirect(const mesh::Packet& p) {
  switch (p.getPayloadType()) {
    case PAYLOAD_TYPE_TRACE:
    case PAYLOAD_TYPE_CONTROL:
    case PAYLOAD_TYPE_MULTIPART:
      return false;
    default:
      return true;
  }
}

struct Target {
  enum Kind : uint8_t { NONE, NODE, GROUP, REPLY_CACHE };
  Kind kind;
  uint8_t hash;
};

inline Target makeTarget(Target::Kind kind, uint8_t hash) {
  Target t;
  t.kind = kind;
  t.hash = hash;
  return t;
}

// Where a flood packet should be published. Call with the packet as received, i.e. BEFORE this
// node appended its own hash to the path.
inline Target floodTarget(const mesh::Packet& p) {
  if (!p.isRouteFlood()) return makeTarget(Target::NONE, 0);
  uint8_t h;
  if (destHash(p, &h)) {
    return isReservedHash(h) ? makeTarget(Target::NONE, 0) : makeTarget(Target::NODE, h);
  }
  if (p.getPayloadType() == PAYLOAD_TYPE_GRP_TXT && p.payload_len >= 1) {
    return makeTarget(Target::GROUP, p.payload[0]);
  }
  return makeTarget(Target::NONE, 0);
}

// Where a direct packet should be published. Call with the packet as transmitted, i.e. AFTER this
// node removed its own hash from the path: the next hop if any, else the destination in the
// payload, else (an ACK has neither) the ReplyCache topics.
inline Target directTarget(const mesh::Packet& p) {
  if (!p.isRouteDirect() || !isBridgeableDirect(p)) return makeTarget(Target::NONE, 0);
  uint8_t h;
  if (p.getPathHashCount() > 0) {
    h = p.path[0];
  } else if (!destHash(p, &h)) {
    return p.getPayloadType() == PAYLOAD_TYPE_ACK ? makeTarget(Target::REPLY_CACHE, 0)
                                                  : makeTarget(Target::NONE, 0);
  }
  return isReservedHash(h) ? makeTarget(Target::NONE, 0) : makeTarget(Target::NODE, h);
}

}  // namespace mqttbridge
