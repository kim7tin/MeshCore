#include "MQTTBridge.h"

#ifdef WITH_MQTT_BRIDGE

#include <esp_system.h>

using namespace mqttbridge;

#ifndef MQTT_TOPIC_PREFIX
#define MQTT_TOPIC_PREFIX "ktm"
#endif
#ifndef MQTT_BROKER1_PORT
#define MQTT_BROKER1_PORT 1883
#endif
#ifndef MQTT_BROKER2_PORT
#define MQTT_BROKER2_PORT 1883
#endif
#ifndef MQTT_BROKER1_USER
#define MQTT_BROKER1_USER ""
#endif
#ifndef MQTT_BROKER1_PASS
#define MQTT_BROKER1_PASS ""
#endif
#ifndef MQTT_BROKER2_USER
#define MQTT_BROKER2_USER ""
#endif
#ifndef MQTT_BROKER2_PASS
#define MQTT_BROKER2_PASS ""
#endif

// Never inject into a pool that radio traffic needs: keep this many packets free.
#define MQTT_MIN_FREE_PACKETS 8
#define MQTT_SOCKET_TIMEOUT_S 2

static uint32_t randBound(uint32_t bound) { return bound ? (esp_random() % bound) : 0; }

MQTTBridge::MQTTBridge(NodePrefs* prefs, mesh::PacketManager* mgr, mesh::RTCClock* rtc)
    : BridgeBase(prefs, mgr, rtc) {
  // Fallback until setSelfIdentity() runs. getEfuseMac() keeps mac[0] in the lowest byte; bytes 0..2
  // are the vendor prefix shared by every board, so take the four NIC-specific bytes 2..5.
  uint64_t mac = ESP.getEfuseMac();
  memcpy(_relay_id, reinterpret_cast<uint8_t*>(&mac) + 2, sizeof(_relay_id));
}

MQTTBridge::~MQTTBridge() { delete _core; }

// Must run before begin(): the core copies the relay id when it is created.
void MQTTBridge::setSelfIdentity(const uint8_t* pub_key, size_t len) {
  if (len < PUB_KEY_SIZE) return;
  relayIdFromPubKey(pub_key, _relay_id);
  _self_hash = pub_key[0];
  _have_self = true;
  if (_core) _core->setSelfHash(_self_hash);
}

void MQTTBridge::begin() {
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(MQTT_WIFI_SSID, MQTT_WIFI_PASSWORD);

  _link_count = 0;
#ifdef MQTT_BROKER1_HOST
  _links[_link_count].host = MQTT_BROKER1_HOST;
  _links[_link_count].port = MQTT_BROKER1_PORT;
  _links[_link_count].user = MQTT_BROKER1_USER;
  _links[_link_count].pass = MQTT_BROKER1_PASS;
  _link_count++;
#endif
#ifdef MQTT_BROKER2_HOST
  _links[_link_count].host = MQTT_BROKER2_HOST;
  _links[_link_count].port = MQTT_BROKER2_PORT;
  _links[_link_count].user = MQTT_BROKER2_USER;
  _links[_link_count].pass = MQTT_BROKER2_PASS;
  _link_count++;
#endif
  for (int i = 0; i < _link_count; i++) {
    Link& l = _links[i];
    l.mqtt.setServer(l.host, l.port);
    l.mqtt.setBufferSize(MQTT_BUFFER);
    l.mqtt.setSocketTimeout(MQTT_SOCKET_TIMEOUT_S);
    l.net.setTimeout(MQTT_SOCKET_TIMEOUT_S);
    l.mqtt.setCallback([this](char* topic, uint8_t* payload, unsigned int len) {
      onMessage(topic, payload, len);
    });
  }

  delete _core;
  _core = new BridgeCore(BridgeConfig::defaults(MQTT_TOPIC_PREFIX), _relay_id, *this, randBound);
  if (_have_self) _core->setSelfHash(_self_hash);
  _initialized = true;
}

void MQTTBridge::end() {
  _initialized = false;
  for (int i = 0; i < _link_count; i++) _links[i].mqtt.disconnect();
  WiFi.mode(WIFI_OFF);
  delete _core;
  _core = nullptr;
}

void MQTTBridge::loop() {
  if (!_initialized || !_core) return;
  uint32_t now = millis();
  bool tried_connect = false;  // at most one (possibly slow) connect attempt per loop() call
  for (int i = 0; i < _link_count; i++) {
    Link& l = _links[i];
    if (l.mqtt.connected()) {
      l.mqtt.loop();
    } else if (!tried_connect && WiFi.status() == WL_CONNECTED && l.backoff.due(now)) {
      tried_connect = true;
      connectLink(l, now);  // blocks for DNS + TCP + CONNACK (bounded by the socket timeout)
    }
  }
  _core->loop(now);
  drainSubQueue();
}

// Network I/O for subscriptions happens here, never while the radio is being serviced.
void MQTTBridge::drainSubQueue() {
  if (_subq.takeOverflow()) {  // requests were lost: bring every connected broker up to date
    for (int i = 0; i < _link_count; i++) {
      if (_links[i].mqtt.connected()) subscribeAll(_links[i]);
    }
  }
  mqttbridge::SubOp op;
  for (int budget = 8; budget > 0 && _subq.pop(&op); budget--) {
    char t[32];
    if (!makeTopic(MQTT_TOPIC_PREFIX, op.kind, op.hash, t, sizeof(t))) continue;
    for (int i = 0; i < _link_count; i++) {
      if (!_links[i].mqtt.connected()) continue;
      if (op.subscribe) {
        _links[i].mqtt.subscribe(t);
      } else {
        _links[i].mqtt.unsubscribe(t);
      }
    }
  }
}

void MQTTBridge::connectLink(Link& l, uint32_t now) {
  char cid[24];
  snprintf(cid, sizeof(cid), "mc-%02x%02x%02x%02x", _relay_id[0], _relay_id[1], _relay_id[2],
           _relay_id[3]);
  bool ok = l.user[0] ? l.mqtt.connect(cid, l.user, l.pass) : l.mqtt.connect(cid);
  if (!ok) {
    l.backoff.fail(now);
    return;
  }
  l.backoff.success();
  subscribeAll(l);
}

// After (re)connecting, subscribe to the group channels and to every neighbor still alive.
void MQTTBridge::subscribeAll(Link& l) {
  char t[32];
  uint8_t hashes[256];
  int n = _core->groupChannels(hashes, 4);
  for (int i = 0; i < n; i++) {
    if (makeTopic(MQTT_TOPIC_PREFIX, TopicKind::GROUP, hashes[i], t, sizeof(t))) l.mqtt.subscribe(t);
  }
  n = _core->activeNeighbors(hashes, sizeof(hashes));
  for (int i = 0; i < n; i++) {
    if (makeTopic(MQTT_TOPIC_PREFIX, TopicKind::NODE, hashes[i], t, sizeof(t))) l.mqtt.subscribe(t);
  }
}

void MQTTBridge::onRadioRx(mesh::Packet* packet) {
  if (_initialized && _core) _core->onRadioRx(*packet, millis());
}

void MQTTBridge::onRadioTx(mesh::Packet* packet) {
  if (_initialized && _core) _core->onRadioTx(*packet, millis());
}

// Called from the radio path (onRadioRx) and from loop(): only queue, the network work is deferred.
void MQTTBridge::subscribe(TopicKind kind, uint8_t hash) { _subq.push(true, kind, hash); }

void MQTTBridge::unsubscribe(TopicKind kind, uint8_t hash) { _subq.push(false, kind, hash); }

void MQTTBridge::publish(TopicKind kind, uint8_t hash, const uint8_t* envelope, size_t len) {
  char t[32];
  if (!makeTopic(MQTT_TOPIC_PREFIX, kind, hash, t, sizeof(t))) return;
  for (int i = 0; i < _link_count; i++) {
    if (_links[i].mqtt.connected()) _links[i].mqtt.publish(t, envelope, (unsigned int)len, false);
  }
}

void MQTTBridge::onMessage(const char* topic, const uint8_t* payload, unsigned int len) {
  if (!_core) return;
  if (_prefs->disable_fwd) return;  // "repeat off": a repeater that does not forward must not inject
  TopicKind kind;
  uint8_t hash;
  if (!parseTopic(MQTT_TOPIC_PREFIX, topic, &kind, &hash)) return;
  uint32_t now = millis();
  Inject r = _core->onMqtt(kind, hash, payload, len, now);
  if (r.kind == InjectKind::NONE) return;
  if (_mgr->getFreeCount() < MQTT_MIN_FREE_PACKETS) return;  // keep room for radio traffic

  mesh::Packet* p = _mgr->allocNew();
  if (!p) return;
  if (!p->readFrom(r.raw, (uint8_t)r.raw_len)) {
    _mgr->free(p);
    return;
  }
  p->_snr = 0;  // allocNew() does not reset it; MQTT packets were not received on this radio
  if (r.kind == InjectKind::FLOOD_INBOUND) {
    _mgr->queueInbound(p, now);  // normal receive path: dedupe, append our hash, delay, forward
  } else {
    _mgr->queueOutbound(p, 0, now + randBound(250));  // direct: next hop is not us, send as-is
  }
}

#endif  // WITH_MQTT_BRIDGE
