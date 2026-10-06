#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace mqttbridge {

struct BrokerConfig {
  char host[64];
  uint16_t port;
  char user[32];
  char pass[64];
};

// Runtime settings of the MQTT bridge: WiFi, up to two brokers, topic prefix. Keys are the part of
// the CLI setting after "mqtt." ("set mqtt.ssid ..." -> key "ssid"). Values are validated here;
// the file format is plain "key=value" lines so it can be edited by hand.
//
//   ssid wifipass | host port user pass | host2 port2 user2 pass2 | prefix group
//
// Secrets (wifipass, pass, pass2) are masked by get() but stored as is by serialize().
class MqttConfig {
public:
  static const int MAX_BROKERS = 2;

  char wifi_ssid[33];
  char wifi_pass[64];
  char prefix[24];
  bool group;
  BrokerConfig broker[MAX_BROKERS];

  MqttConfig() { clear(); }

  void clear() {
    wifi_ssid[0] = 0;
    wifi_pass[0] = 0;
    strcpy(prefix, "meshcore");
    group = true;
    for (int i = 0; i < MAX_BROKERS; i++) {
      broker[i].host[0] = 0;
      broker[i].port = 1883;
      broker[i].user[0] = 0;
      broker[i].pass[0] = 0;
    }
  }

  // The bridge only starts when it knows a WiFi network and a first broker.
  bool configured() const { return wifi_ssid[0] != 0 && broker[0].host[0] != 0; }

  // Applies one setting. On failure returns false and writes a short reason to 'err'.
  bool set(const char* key, const char* value, char* err, size_t errcap) {
    if (!key || !value) return fail(err, errcap, "missing key or value");
    for (const char* p = value; *p; p++) {
      unsigned char c = (unsigned char)*p;
      if (c < 0x20 || c == 0x7F) return fail(err, errcap, "control characters are not allowed");
    }
    size_t len = strlen(value);

    if (strcmp(key, "ssid") == 0) return copyStr(wifi_ssid, sizeof(wifi_ssid), value, len, "ssid max 32 chars", err, errcap);
    if (strcmp(key, "wifipass") == 0) return copyStr(wifi_pass, sizeof(wifi_pass), value, len, "wifipass max 63 chars", err, errcap);
    if (strcmp(key, "prefix") == 0) {
      if (!validPrefix(value, len)) return fail(err, errcap, "prefix: 1-20 chars a-z 0-9 _ - /, no leading/trailing /");
      strcpy(prefix, value);
      return true;
    }
    if (strcmp(key, "group") == 0) {
      if (strcmp(value, "on") == 0) { group = true; return true; }
      if (strcmp(value, "off") == 0) { group = false; return true; }
      return fail(err, errcap, "group: on or off");
    }
    int idx;
    const char* field = brokerField(key, &idx);
    if (!field) return fail(err, errcap, "unknown mqtt setting");
    BrokerConfig& b = broker[idx];
    if (strcmp(field, "host") == 0) {
      if (!validHost(value, len)) return fail(err, errcap, "host: max 63 chars a-z 0-9 . - _ :");
      strcpy(b.host, value);
      return true;
    }
    if (strcmp(field, "port") == 0) {
      char* end = nullptr;
      long v = strtol(value, &end, 10);
      if (len == 0 || *end != 0 || v < 1 || v > 65535) return fail(err, errcap, "port: 1-65535");
      b.port = (uint16_t)v;
      return true;
    }
    if (strcmp(field, "user") == 0) return copyStr(b.user, sizeof(b.user), value, len, "user max 31 chars", err, errcap);
    return copyStr(b.pass, sizeof(b.pass), value, len, "pass max 63 chars", err, errcap);  // "pass"
  }

  // Writes the setting for display. Secrets show as "***" and empty values as "(not set)".
  // Returns false for an unknown key or when it does not fit in 'out'.
  bool get(const char* key, char* out, size_t cap) const {
    if (!key || !out || cap == 0) return false;
    out[0] = 0;
    const char* v = nullptr;
    char num[8];
    bool secret = false;
    if (strcmp(key, "ssid") == 0) v = wifi_ssid;
    else if (strcmp(key, "wifipass") == 0) { v = wifi_pass; secret = true; }
    else if (strcmp(key, "prefix") == 0) v = prefix;
    else if (strcmp(key, "group") == 0) v = group ? "on" : "off";
    else {
      int idx;
      const char* field = brokerField(key, &idx);
      if (!field) return false;
      const BrokerConfig& b = broker[idx];
      if (strcmp(field, "host") == 0) v = b.host;
      else if (strcmp(field, "port") == 0) { snprintf(num, sizeof(num), "%u", (unsigned)b.port); v = num; }
      else if (strcmp(field, "user") == 0) v = b.user;
      else { v = b.pass; secret = true; }
    }
    const char* shown = (*v == 0) ? "(not set)" : (secret ? "***" : v);
    if (strlen(shown) >= cap) return false;
    strcpy(out, shown);
    return true;
  }

  // Writes the settings as "key=value\n" lines, secrets included. Returns the length, or 0 if it
  // does not fit in 'cap' (including the terminating NUL).
  size_t serialize(char* out, size_t cap) const {
    if (!out || cap == 0) return 0;
    size_t n = 0;
    bool ok = true;
    ok = ok && addLine(out, cap, n, "ssid", wifi_ssid);
    ok = ok && addLine(out, cap, n, "wifipass", wifi_pass);
    ok = ok && addLine(out, cap, n, "prefix", prefix);
    ok = ok && addLine(out, cap, n, "group", group ? "on" : "off");
    static const char* const names[MAX_BROKERS][4] = {{"host", "port", "user", "pass"},
                                                      {"host2", "port2", "user2", "pass2"}};
    for (int i = 0; i < MAX_BROKERS && ok; i++) {
      char num[8];
      snprintf(num, sizeof(num), "%u", (unsigned)broker[i].port);
      ok = ok && addLine(out, cap, n, names[i][0], broker[i].host);
      ok = ok && addLine(out, cap, n, names[i][1], num);
      ok = ok && addLine(out, cap, n, names[i][2], broker[i].user);
      ok = ok && addLine(out, cap, n, names[i][3], broker[i].pass);
    }
    if (!ok) {
      out[0] = 0;
      return 0;
    }
    return n;
  }

  // Applies "key=value" lines on top of the current settings. Bad lines are skipped.
  void parse(const char* text) {
    if (!text) return;
    const char* p = text;
    while (*p) {
      const char* end = strchr(p, '\n');
      size_t len = end ? (size_t)(end - p) : strlen(p);
      if (len > 0 && p[len - 1] == '\r') len--;
      char line[160];
      if (len > 0 && len < sizeof(line)) {
        memcpy(line, p, len);
        line[len] = 0;
        char* eq = strchr(line, '=');
        if (eq && eq != line) {
          *eq = 0;
          char err[8];
          set(line, eq + 1, err, sizeof(err));
        }
      }
      if (!end) break;
      p = end + 1;
    }
  }

private:
  static bool fail(char* err, size_t errcap, const char* msg) {
    if (err && errcap > 0) {
      snprintf(err, errcap, "%s", msg);
    }
    return false;
  }

  static bool copyStr(char* dst, size_t dstcap, const char* v, size_t len, const char* msg, char* err,
                      size_t errcap) {
    if (len >= dstcap) return fail(err, errcap, msg);
    memcpy(dst, v, len + 1);
    return true;
  }

  static bool validHost(const char* v, size_t len) {
    if (len > 63) return false;
    for (size_t i = 0; i < len; i++) {
      char c = v[i];
      bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' ||
                c == '-' || c == '_' || c == ':';
      if (!ok) return false;
    }
    return true;
  }

  static bool validPrefix(const char* v, size_t len) {
    if (len < 1 || len > 20 || v[0] == '/' || v[len - 1] == '/') return false;
    for (size_t i = 0; i < len; i++) {
      char c = v[i];
      bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
                c == '-' || c == '/';
      if (!ok) return false;
      if (c == '/' && v[i + 1] == '/') return false;
    }
    return true;
  }

  // "host"/"host2" ... -> field name without the digit, broker index 0 or 1; nullptr if not a broker key.
  static const char* brokerField(const char* key, int* idx) {
    static const char* const fields[] = {"host", "port", "user", "pass"};
    for (int f = 0; f < 4; f++) {
      size_t n = strlen(fields[f]);
      if (strncmp(key, fields[f], n) != 0) continue;
      if (key[n] == 0) { *idx = 0; return fields[f]; }
      if (key[n] == '2' && key[n + 1] == 0) { *idx = 1; return fields[f]; }
    }
    return nullptr;
  }

  static bool addLine(char* out, size_t cap, size_t& n, const char* key, const char* value) {
    size_t need = strlen(key) + 1 + strlen(value) + 1;
    if (n + need >= cap) return false;  // keep room for the terminating NUL
    n += (size_t)snprintf(out + n, cap - n, "%s=%s\n", key, value);
    return true;
  }
};

}  // namespace mqttbridge
