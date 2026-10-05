#pragma once

#include <PubSubClient.h>
#include <WiFi.h>

#include "helpers/bridges/BridgeBase.h"
#include "helpers/bridges/mqtt/Backoff.h"
#include "helpers/bridges/mqtt/BridgeCore.h"

/**
 * @brief Bridges repeater traffic to MQTT brokers (up to two) over WiFi.
 *
 * The decision logic lives in mqttbridge::BridgeCore (host-tested). This class only moves bytes:
 * radio events in via onRadioRx()/onRadioTx(), MQTT messages in via PubSubClient callbacks, and
 * packets out via the repeater's PacketManager.
 *
 * Configured with build flags: MQTT_WIFI_SSID, MQTT_WIFI_PASSWORD, MQTT_TOPIC_PREFIX,
 * MQTT_BROKER1_HOST/PORT/USER/PASS and optionally the same for MQTT_BROKER2_*.
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

private:
  static const int MAX_LINKS = 2;
  static const int MQTT_BUFFER = 320;  // 6-byte envelope + 255-byte packet + topic + header

  struct Link {
    WiFiClient net;
    PubSubClient mqtt;
    const char* host = nullptr;
    uint16_t port = 1883;
    const char* user = "";
    const char* pass = "";
    mqttbridge::Backoff backoff;
    Link() : mqtt(net) {}
  };

  Link _links[MAX_LINKS];
  int _link_count = 0;
  mqttbridge::BridgeCore* _core = nullptr;
  uint8_t _relay_id[4];
  uint8_t _self_hash = 0;
  bool _have_self = false;

  // BridgeSink
  void subscribe(mqttbridge::TopicKind kind, uint8_t hash) override;
  void unsubscribe(mqttbridge::TopicKind kind, uint8_t hash) override;
  void publish(mqttbridge::TopicKind kind, uint8_t hash, const uint8_t* envelope,
               size_t len) override;

  void connectLink(Link& link, uint32_t now);
  void subscribeAll(Link& link);
  void onMessage(const char* topic, const uint8_t* payload, unsigned int len);
};
