#include "MQTTBridge.h"

#ifdef WITH_MQTT_BRIDGE

#include <esp_system.h>

#include "helpers/bridges/mqtt/MqttStatus.h"

using namespace mqttbridge;

#define MQTT_CONFIG_FILE "/mqtt.cfg"

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

// ---- settings ---------------------------------------------------------------------------------

// The MQTT_* build flags are optional defaults, so a firmware image can ship with no secrets at all.
void MQTTBridge::applyBuildDefaults() {
  char err[8];
#ifdef MQTT_WIFI_SSID
  _cfg.set("ssid", MQTT_WIFI_SSID, err, sizeof(err));
#endif
#ifdef MQTT_WIFI_PASSWORD
  _cfg.set("wifipass", MQTT_WIFI_PASSWORD, err, sizeof(err));
#endif
#ifdef MQTT_TOPIC_PREFIX
  _cfg.set("prefix", MQTT_TOPIC_PREFIX, err, sizeof(err));
#endif
#ifdef MQTT_BROKER1_HOST
  _cfg.set("host", MQTT_BROKER1_HOST, err, sizeof(err));
#endif
#ifdef MQTT_BROKER1_PORT
  { char p[8]; snprintf(p, sizeof(p), "%d", (int)(MQTT_BROKER1_PORT)); _cfg.set("port", p, err, sizeof(err)); }
#endif
#ifdef MQTT_BROKER1_USER
  _cfg.set("user", MQTT_BROKER1_USER, err, sizeof(err));
#endif
#ifdef MQTT_BROKER1_PASS
  _cfg.set("pass", MQTT_BROKER1_PASS, err, sizeof(err));
#endif
#ifdef MQTT_BROKER2_HOST
  _cfg.set("host2", MQTT_BROKER2_HOST, err, sizeof(err));
#endif
#ifdef MQTT_BROKER2_PORT
  { char p[8]; snprintf(p, sizeof(p), "%d", (int)(MQTT_BROKER2_PORT)); _cfg.set("port2", p, err, sizeof(err)); }
#endif
#ifdef MQTT_BROKER2_USER
  _cfg.set("user2", MQTT_BROKER2_USER, err, sizeof(err));
#endif
#ifdef MQTT_BROKER2_PASS
  _cfg.set("pass2", MQTT_BROKER2_PASS, err, sizeof(err));
#endif
}

void MQTTBridge::loadConfig() {
  _cfg.clear();
  applyBuildDefaults();
  if (!_fs) return;
  File f = _fs->open(MQTT_CONFIG_FILE, "r");
  if (!f) return;
  char buf[640];
  size_t n = f.readBytes(buf, sizeof(buf) - 1);
  buf[n] = 0;
  f.close();
  _cfg.parse(buf);
}

bool MQTTBridge::saveConfig() {
  if (!_fs) return false;
  char buf[640];
  size_t n = _cfg.serialize(buf, sizeof(buf));
  if (n == 0) return false;
  File f = _fs->open(MQTT_CONFIG_FILE, "w", true);
  if (!f) return false;
  bool ok = f.write((const uint8_t*)buf, n) == n;
  f.close();
  return ok;
}

// "set mqtt.<key> <value>": the bridge is stopped while the setting changes, then started again
// with the new values (if it is enabled and fully configured).
bool MQTTBridge::handleSetConfig(const char* config, char* reply) {
  if (strncmp(config, "mqtt.", 5) != 0) return false;
  const char* key = config + 5;
  const char* sp = strchr(key, ' ');
  size_t klen = sp ? (size_t)(sp - key) : strlen(key);
  char k[16];
  if (klen == 0 || klen >= sizeof(k)) {
    strcpy(reply, "Error: unknown mqtt setting");
    return true;
  }
  memcpy(k, key, klen);
  k[klen] = 0;
  const char* value = sp ? sp + 1 : "";  // the rest of the line, spaces included

  bool was_running = _initialized;
  if (was_running) end();
  loadConfig();
  char err[48];
  if (!_cfg.set(k, value, err, sizeof(err))) {
    snprintf(reply, 100, "Error: %s", err);
  } else if (!saveConfig()) {
    strcpy(reply, "Error: could not save /mqtt.cfg");
  } else {
    strcpy(reply, "OK");
  }
  if (_prefs->bridge_enabled) {
    begin();
    if (_initialized && strlen(reply) == 2) strcpy(reply, "OK - bridge restarted");
    else if (!_initialized && strlen(reply) == 2) strcpy(reply, "OK - set mqtt.ssid and mqtt.host to start");
  }
  return true;
}

// Read-only status keys: wifi, broker1, broker2, stats, neighbors. Returns false for other keys.
bool MQTTBridge::getStatus(const char* key, char* reply) {
  if (strcmp(key, "wifi") == 0) {
    if (!_initialized) {
      strcpy(reply, _cfg.configured() ? "> wifi off (bridge not running)" : "> wifi off (not configured)");
    } else if (WiFi.status() == WL_CONNECTED) {
      snprintf(reply, 150, "> wifi connected, ssid %s, ip %s, rssi %d dBm", _cfg.wifi_ssid,
               WiFi.localIP().toString().c_str(), (int)WiFi.RSSI());
    } else {
      snprintf(reply, 150, "> wifi %s, ssid %s", wifiStatusText((int)WiFi.status()), _cfg.wifi_ssid);
    }
    return true;
  }
  if (strcmp(key, "broker1") == 0 || strcmp(key, "broker2") == 0) {
    int idx = key[6] - '1';
    const BrokerConfig& b = _cfg.broker[idx];
    if (b.host[0] == 0) {
      snprintf(reply, 150, "> broker%d not set", idx + 1);
      return true;
    }
    int link = -1;  // links are packed: only brokers with a host have one
    if (_initialized) {
      link = 0;
      for (int i = 0; i < idx; i++) {
        if (_cfg.broker[i].host[0] != 0) link++;
      }
    }
    if (link < 0 || link >= _link_count) {
      snprintf(reply, 150, "> broker%d %s:%u off (bridge not running)", idx + 1, b.host, (unsigned)b.port);
    } else if (_links[link].mqtt.connected()) {
      snprintf(reply, 150, "> broker%d %s:%u connected", idx + 1, b.host, (unsigned)b.port);
    } else if (WiFi.status() != WL_CONNECTED) {
      snprintf(reply, 150, "> broker%d %s:%u down: no wifi", idx + 1, b.host, (unsigned)b.port);
    } else {
      snprintf(reply, 150, "> broker%d %s:%u down: %s", idx + 1, b.host, (unsigned)b.port,
               mqttStateText(_links[link].mqtt.state()));
    }
    return true;
  }
  if (strcmp(key, "stats") == 0) {
    if (!_initialized || !_core) {
      strcpy(reply, "> bridge off");
    } else if (formatStats(_core->stats(), reply, 150) == 0) {
      strcpy(reply, "> stats unavailable");
    }
    return true;
  }
  if (strcmp(key, "neighbors") == 0) {
    if (!_initialized || !_core) {
      strcpy(reply, "> bridge off");
      return true;
    }
    uint8_t hashes[256];
    int n = _core->activeNeighbors(hashes, sizeof(hashes));
    int len = snprintf(reply, 150, "> %d neighbors:", n);
    int shown = 0;
    for (; shown < n && len < 140; shown++) len += snprintf(reply + len, 150 - len, " %02x", hashes[shown]);
    if (shown < n) snprintf(reply + len, 150 - len, " ...");
    return true;
  }
  return false;
}

bool MQTTBridge::handleGetConfig(const char* config, char* reply) {
  if (strncmp(config, "mqtt", 4) != 0) return false;
  if (!_initialized) loadConfig();  // while running _cfg is already current
  if (config[4] == '.') {
    if (getStatus(config + 5, reply)) return true;
    char val[96];
    if (!_cfg.get(config + 5, val, sizeof(val))) {
      strcpy(reply, "Error: unknown mqtt setting");
    } else {
      snprintf(reply, 120, "> %s", val);
    }
    return true;
  }
  if (config[4] != 0) return false;

  // "get mqtt": one-line status
  if (!_cfg.configured()) {
    strcpy(reply, "> not configured: set mqtt.ssid and mqtt.host");
    return true;
  }
  const char* wifi = !_initialized ? "off" : (WiFi.status() == WL_CONNECTED ? "up" : "down");
  int n = snprintf(reply, 150, "> wifi %s (%s)", wifi, _cfg.wifi_ssid);
  for (int i = 0; i < MAX_LINKS && n > 0 && n < 130; i++) {
    if (_cfg.broker[i].host[0] == 0) continue;
    bool up = _initialized && i < _link_count && _links[i].mqtt.connected();
    n += snprintf(reply + n, 150 - n, ", broker%d %s:%u %s", i + 1, _cfg.broker[i].host,
                  (unsigned)_cfg.broker[i].port, up ? "up" : "down");
  }
  return true;
}

// ---- lifecycle ---------------------------------------------------------------------------------

void MQTTBridge::begin() {
  loadConfig();
  if (!_cfg.configured()) {
    _initialized = false;
    MESH_DEBUG_PRINTLN("MQTT bridge off: set mqtt.ssid, mqtt.wifipass, mqtt.host (see docs/mqtt_bridge.md)");
    return;
  }

  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(_cfg.wifi_ssid, _cfg.wifi_pass);

  _link_count = 0;
  for (int i = 0; i < MAX_LINKS; i++) {
    if (_cfg.broker[i].host[0] == 0) continue;
    Link& l = _links[_link_count++];
    l.host = _cfg.broker[i].host;
    l.port = _cfg.broker[i].port;
    l.user = _cfg.broker[i].user;
    l.pass = _cfg.broker[i].pass;
    l.up = false;
    l.mqtt.setServer(l.host, l.port);
    l.mqtt.setBufferSize(MQTT_BUFFER);
    l.mqtt.setSocketTimeout(MQTT_SOCKET_TIMEOUT_S);
    l.net.setTimeout(MQTT_SOCKET_TIMEOUT_S);
    l.mqtt.setCallback([this](char* topic, uint8_t* payload, unsigned int len) {
      onMessage(topic, payload, len);
    });
  }

  BridgeConfig cfg = BridgeConfig::defaults(_cfg.prefix);
  if (!_cfg.group) cfg.group_count = 0;
  delete _core;
  _core = new BridgeCore(cfg, _relay_id, *this, randBound);
  if (_have_self) _core->setSelfHash(_self_hash);
  _initialized = true;
}

void MQTTBridge::end() {
  _initialized = false;
  for (int i = 0; i < _link_count; i++) _links[i].mqtt.disconnect();
  _link_count = 0;
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
    if (l.up != l.mqtt.connected()) {
      l.up = l.mqtt.connected();
      MESH_DEBUG_PRINTLN("MQTT broker %d %s", i + 1, l.up ? "connected" : "disconnected");
    }
  }
  _core->loop(now);
  drainSubQueue();
}

void MQTTBridge::connectLink(Link& l, uint32_t now) {
  char cid[24];
  snprintf(cid, sizeof(cid), "mc-%02x%02x%02x%02x", _relay_id[0], _relay_id[1], _relay_id[2],
           _relay_id[3]);
  bool ok = l.user[0] ? l.mqtt.connect(cid, l.user, l.pass) : l.mqtt.connect(cid);
  if (!ok) {
    MESH_DEBUG_PRINTLN("MQTT connect to %s failed (state %d)", l.host, l.mqtt.state());
    l.backoff.fail(now);
    return;
  }
  l.backoff.success();
  subscribeAll(l);
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
    char t[48];
    if (!makeTopic(_cfg.prefix, op.kind, op.hash, t, sizeof(t))) continue;
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

// After (re)connecting, subscribe to the group channels and to every neighbor still alive.
void MQTTBridge::subscribeAll(Link& l) {
  char t[48];
  uint8_t hashes[256];
  int n = _core->groupChannels(hashes, 4);
  for (int i = 0; i < n; i++) {
    if (makeTopic(_cfg.prefix, TopicKind::GROUP, hashes[i], t, sizeof(t))) l.mqtt.subscribe(t);
  }
  n = _core->activeNeighbors(hashes, sizeof(hashes));
  for (int i = 0; i < n; i++) {
    if (makeTopic(_cfg.prefix, TopicKind::NODE, hashes[i], t, sizeof(t))) l.mqtt.subscribe(t);
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
  char t[48];
  if (!makeTopic(_cfg.prefix, kind, hash, t, sizeof(t))) return;
  for (int i = 0; i < _link_count; i++) {
    if (_links[i].mqtt.connected()) _links[i].mqtt.publish(t, envelope, (unsigned int)len, false);
  }
}

void MQTTBridge::onMessage(const char* topic, const uint8_t* payload, unsigned int len) {
  if (!_core) return;
  if (_prefs->disable_fwd) return;  // "repeat off": a repeater that does not forward must not inject
  TopicKind kind;
  uint8_t hash;
  if (!parseTopic(_cfg.prefix, topic, &kind, &hash)) return;
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
