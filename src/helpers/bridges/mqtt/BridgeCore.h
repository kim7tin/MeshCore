#pragma once

#include <Packet.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "Backoff.h"
#include "KeyRing.h"
#include "MqttEnvelope.h"
#include "MqttTopics.h"
#include "NeighborTable.h"
#include "PacketRules.h"
#include "PublishQueue.h"
#include "ReplyCache.h"
#include "WindowLimiter.h"

namespace mqttbridge {

struct BridgeConfig {
  const char* prefix;
  uint32_t neighbor_ttl_ms;
  uint32_t reply_ttl_ms;
  uint32_t publish_jitter_ms;  // publishes wait a random 0..jitter ms so duplicates can be cancelled
  uint16_t pub_per_min;
  uint16_t inject_per_min;
  uint16_t group_per_min;  // limit for group packets, applied to publish and inject separately
  uint8_t group_channels[4];
  uint8_t group_count;

  static BridgeConfig defaults(const char* prefix) {
    BridgeConfig c;
    c.prefix = prefix;
    c.neighbor_ttl_ms = 180000UL;
    c.reply_ttl_ms = 15000UL;
    c.publish_jitter_ms = 1000UL;
    c.pub_per_min = 120;
    c.inject_per_min = 120;
    c.group_per_min = 6;
    memset(c.group_channels, 0, sizeof(c.group_channels));
    c.group_channels[0] = 0x11;  // Public channel (first byte of SHA-256 of its well-known key)
    c.group_count = 1;
    return c;
  }
};

// Implemented by the transport adapter (ESP32) and by tests.
class BridgeSink {
public:
  virtual ~BridgeSink() {}
  virtual void subscribe(TopicKind kind, uint8_t hash) = 0;
  virtual void unsubscribe(TopicKind kind, uint8_t hash) = 0;
  virtual void publish(TopicKind kind, uint8_t hash, const uint8_t* envelope, size_t len) = 0;
};

enum class InjectKind : uint8_t { NONE, FLOOD_INBOUND, DIRECT_OUTBOUND };

struct Inject {
  InjectKind kind;
  const uint8_t* raw;  // points into the MQTT payload handed to onMqtt(); valid until it returns
  size_t raw_len;
};

struct BridgeStats {
  uint32_t published, injected_flood, injected_direct;
  uint32_t dropped_ratelimit, dropped_queue_full, dropped_duplicate;
  uint32_t suppressed_publish, own_dropped, bad_envelope, rejected_policy;
};

typedef uint32_t (*RandFn)(uint32_t bound);  // returns a value in [0, bound)

// The bridge policy. No Arduino, WiFi or MQTT dependency: the adapter feeds it radio events
// and MQTT messages and carries out what it asks for through BridgeSink / Inject.
class BridgeCore {
  static const int KEY_LEN = 8;  // MAX_HASH_SIZE

  BridgeConfig _cfg;
  uint8_t _relay_id[4];
  BridgeSink& _sink;
  RandFn _rand;
  uint8_t _self;
  bool _have_self;
  NeighborTable _neighbors;
  ReplyCache _reply;
  WindowLimiter _pub_lim, _group_pub_lim, _inj_lim, _group_inj_lim;
  PublishQueue _queue;
  KeyRing _injected;  // packets that came from MQTT: never published back
  KeyRing _sent;      // packets this node transmitted: never injected again
  KeyRing _heard;     // packets heard on the radio: the MQTT copy would only duplicate them
  BridgeStats _stats;

  bool groupEnabled(uint8_t h) const {
    for (int i = 0; i < _cfg.group_count && i < 4; i++) {
      if (_cfg.group_channels[i] == h) return true;
    }
    return false;
  }

  void schedule(const Target& t, const mesh::Packet& q, const uint8_t key[KEY_LEN], uint32_t now) {
    uint8_t hashes[8];
    int n = 0;
    TopicKind kind = TopicKind::NODE;
    if (t.kind == Target::NODE) {
      hashes[n++] = t.hash;
    } else if (t.kind == Target::GROUP) {
      if (!groupEnabled(t.hash)) return;
      kind = TopicKind::GROUP;
      hashes[n++] = t.hash;
    } else if (t.kind == Target::REPLY_CACHE) {
      n = _reply.collect(now, hashes, 8);
    }
    if (n == 0) return;

    WindowLimiter& lim = (kind == TopicKind::GROUP) ? _group_pub_lim : _pub_lim;
    if (!lim.allow(now)) {
      _stats.dropped_ratelimit++;
      return;
    }
    uint8_t raw[256];
    size_t rl = q.writeTo(raw);
    uint32_t due = now + (_cfg.publish_jitter_ms ? _rand(_cfg.publish_jitter_ms + 1) : 0);
    for (int i = 0; i < n; i++) {
      if (!_queue.push(kind, hashes[i], key, raw, rl, due)) _stats.dropped_queue_full++;
    }
  }

public:
  BridgeCore(const BridgeConfig& cfg, const uint8_t relay_id[4], BridgeSink& sink, RandFn rand_fn)
      : _cfg(cfg), _sink(sink), _rand(rand_fn), _self(0), _have_self(false),
        _neighbors(cfg.neighbor_ttl_ms), _reply(cfg.reply_ttl_ms),
        _pub_lim(cfg.pub_per_min, 60000UL), _group_pub_lim(cfg.group_per_min, 60000UL),
        _inj_lim(cfg.inject_per_min, 60000UL), _group_inj_lim(cfg.group_per_min, 60000UL) {
    memcpy(_relay_id, relay_id, 4);
    memset(&_stats, 0, sizeof(_stats));
  }

  // First byte of this node's public key (what repeaters append to flood paths).
  void setSelfHash(uint8_t hash) {
    _self = hash;
    _have_self = true;
  }

  // Call for every packet received from the radio, before the repeater logic runs (also for
  // duplicates). Keeps the neighbor subscriptions fresh.
  void onRadioRx(const mesh::Packet& p, uint32_t now) {
    uint8_t key[KEY_LEN];
    p.calculatePacketHash(key);
    _heard.add(key);
    uint8_t h;
    if (!nearbyHash(p, &h)) return;
    if (_have_self && h == _self) return;
    if (_neighbors.touch(h, now)) _sink.subscribe(TopicKind::NODE, h);
  }

  // Call for every packet this node transmits (forwarded, injected or originated).
  void onRadioTx(const mesh::Packet& p, uint32_t now) {
    uint8_t key[KEY_LEN];
    p.calculatePacketHash(key);
    if (_injected.contains(key)) return;  // came from MQTT: never publish it back
    _sent.add(key);

    if (p.isRouteFlood()) {
      uint8_t n = p.getPathHashCount();
      if (n == 0 || !_have_self) return;                  // originated here, not forwarded
      if (p.path[(n - 1) * p.getPathHashSize()] != _self) return;
      mesh::Packet q = p;
      q.setPathHashCount((uint8_t)(n - 1));               // undo the hash this node appended
      schedule(floodTarget(q), q, key, now);
    } else if (p.isRouteDirect()) {
      schedule(directTarget(p), p, key, now);
    }
  }

  // Call for every MQTT message received on a subscribed topic.
  Inject onMqtt(TopicKind kind, uint8_t hash, const uint8_t* payload, size_t len, uint32_t now) {
    Inject none = {InjectKind::NONE, nullptr, 0};
    uint8_t from[4];
    const uint8_t* raw;
    size_t raw_len;
    if (!decodeEnvelope(payload, len, from, &raw, &raw_len) || raw_len > 255) {
      _stats.bad_envelope++;
      return none;
    }
    if (memcmp(from, _relay_id, 4) == 0) {
      _stats.own_dropped++;
      return none;
    }
    mesh::Packet p;
    // readFrom() trusts its input; check the bounds ourselves first (untrusted network data).
    if (!isWellFormedRaw(raw, raw_len) || !p.readFrom(raw, (uint8_t)raw_len)) {
      _stats.bad_envelope++;
      return none;
    }
    // MQTT is untrusted: only carry what a publisher of this bridge emits, on the right topic.
    bool group = p.getPayloadType() == PAYLOAD_TYPE_GRP_TXT;
    if (p.getPayloadVer() > PAYLOAD_VER_1 || !isBridgedType(p) || !topicMatches(kind, hash, p) ||
        (group && !groupEnabled(p.payload[0]))) {
      _stats.rejected_policy++;
      return none;
    }
    uint8_t key[KEY_LEN];
    p.calculatePacketHash(key);
    _stats.suppressed_publish += (uint32_t)_queue.cancel(key);  // someone else already published it
    if (_injected.contains(key) || _sent.contains(key) || _heard.contains(key)) {
      _stats.dropped_duplicate++;
      return none;
    }
    // Remember it even if the limiter drops it below, so that a radio copy forwarded later is
    // still never published back.
    _injected.add(key);
    if (!(group ? _group_inj_lim : _inj_lim).allow(now)) {
      _stats.dropped_ratelimit++;
      return none;
    }
    uint8_t s;
    if (srcHash(p, &s) && !isReservedHash(s)) _reply.note(s, now);

    Inject r;
    r.raw = raw;
    r.raw_len = raw_len;
    if (p.isRouteFlood()) {
      r.kind = InjectKind::FLOOD_INBOUND;
      _stats.injected_flood++;
    } else {
      r.kind = InjectKind::DIRECT_OUTBOUND;
      _stats.injected_direct++;
    }
    return r;
  }

  // Call regularly from the main loop: expires neighbors and sends due publishes.
  void loop(uint32_t now) {
    _reply.prune(now);
    uint8_t expired[16];
    int n;
    while ((n = _neighbors.expire(now, expired, 16)) > 0) {
      for (int i = 0; i < n; i++) _sink.unsubscribe(TopicKind::NODE, expired[i]);
    }
    PendingPublish e;
    while (_queue.popDue(now, &e)) {
      uint8_t env[ENVELOPE_HEADER_LEN + 255];
      size_t el = encodeEnvelope(_relay_id, e.data, e.len, env, sizeof(env));
      if (el == 0) continue;
      _sink.publish(e.kind, e.hash, env, el);
      _stats.published++;
    }
  }

  int activeNeighbors(uint8_t* out, int cap) const { return _neighbors.collectActive(out, cap); }

  int groupChannels(uint8_t* out, int cap) const {
    int n = 0;
    for (int i = 0; i < _cfg.group_count && i < 4 && n < cap; i++) out[n++] = _cfg.group_channels[i];
    return n;
  }

  const BridgeStats& stats() const { return _stats; }
};

}  // namespace mqttbridge
