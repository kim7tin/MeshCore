#pragma once

#include <FS.h>
#include <PubSubClient.h>
#include <WiFi.h>

#include "helpers/bridges/BridgeBase.h"
#include "helpers/bridges/mqtt/Backoff.h"
#include "helpers/bridges/mqtt/BridgeCore.h"
#include "helpers/bridges/mqtt/MqttConfig.h"
#include "helpers/bridges/mqtt/RelayId.h"
#include "helpers/bridges/mqtt/SubQueue.h"

/**
 * @brief Bridges repeater traffic to MQTT brokers (up to two) over WiFi.
 *
 * The decision logic lives in mqttbridge::BridgeCore (host-tested). This class only moves bytes:
 * radio events in via onRadioRx()/onRadioTx(), MQTT messages in via PubSubClient callbacks, and
 * packets out via the repeater's PacketManager.
 *
 * Settings (WiFi, brokers, topic prefix) are changed with the CLI ("set mqtt.ssid ...", see
 * docs/mqtt_bridge.md) and kept in /mqtt.cfg. The MQTT_* build flags only provide defaults.
 * Until a WiFi network and a first broker are set the bridge stays off.
 */
class MQTTBridge : public BridgeBase, private mqttbridge::BridgeSink {
public:
  MQTTBridge(NodePrefs* prefs, mesh::PacketManager* mgr, mesh::RTCClock* rtc);
  ~MQTTBridge() override;

  void begin() override;
  void end() override;
  void loop() override;
  void sendPacket(mesh::Packet* packet) override { (void)packet; }  // uses onRadioRx/onRadioTx
  void onPacketReceived(mesh::Packet* packet) override { handleReceivedPacket(packet); }
  void onRadioRx(mesh::Packet* packet) override;
  void onRadioTx(mesh::Packet* packet) override;
  void setSelfIdentity(const uint8_t* pub_key, size_t len) override;

  // Where /mqtt.cfg lives. Call before begin().
  void setFileSystem(fs::FS* fs) { _fs = fs; }

  // CLI plumbing: "set mqtt.<key> <value>" and "get mqtt[.<key>]". 'config' is the text after
  // "set " / "get ". Return false if it is not an mqtt setting.
  bool handleSetConfig(const char* config, char* reply);
  bool handleGetConfig(const char* config, char* reply);

private:
  static const int MAX_LINKS = mqttbridge::MqttConfig::MAX_BROKERS;
  static const int MQTT_BUFFER = 320;  // 6-byte envelope + 255-byte packet + topic + header

  struct Link {
    WiFiClient net;
    PubSubClient mqtt;
    const char* host = nullptr;  // point into _cfg, which is not touched while the bridge runs
    uint16_t port = 1883;
    const char* user = "";
    const char* pass = "";
    bool up = false;
    mqttbridge::Backoff backoff;
    Link() : mqtt(net) {}
  };

  Link _links[MAX_LINKS];
  int _link_count = 0;
  mqttbridge::BridgeCore* _core = nullptr;
  mqttbridge::SubQueue _subq;  // subscribe/unsubscribe requests, drained from loop()
  mqttbridge::MqttConfig _cfg;
  fs::FS* _fs = nullptr;
  uint8_t _relay_id[4];
  uint8_t _self_hash = 0;
  bool _have_self = false;

  void applyBuildDefaults();
  void loadConfig();
  bool saveConfig();

  // BridgeSink
  void subscribe(mqttbridge::TopicKind kind, uint8_t hash) override;
  void unsubscribe(mqttbridge::TopicKind kind, uint8_t hash) override;
  void publish(mqttbridge::TopicKind kind, uint8_t hash, const uint8_t* envelope,
               size_t len) override;

  void connectLink(Link& link, uint32_t now);
  void drainSubQueue();
  void subscribeAll(Link& link);
  void onMessage(const char* topic, const uint8_t* payload, unsigned int len);
};
