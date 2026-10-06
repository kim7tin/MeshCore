#include <gtest/gtest.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include "helpers/bridges/mqtt/MqttStatus.h"

using namespace mqttbridge;

TEST(MqttStatus, WifiStatusTextCoversTheArduinoValues) {
  EXPECT_STREQ("idle", wifiStatusText(0));
  EXPECT_STREQ("network not found", wifiStatusText(1));
  EXPECT_STREQ("connected", wifiStatusText(3));
  EXPECT_STREQ("connect failed (wrong password?)", wifiStatusText(4));
  EXPECT_STREQ("connection lost", wifiStatusText(5));
  EXPECT_STREQ("disconnected", wifiStatusText(6));
  EXPECT_STREQ("unknown", wifiStatusText(42));
  EXPECT_STREQ("unknown", wifiStatusText(-1));
}

TEST(MqttStatus, MqttStateTextCoversPubSubClientStates) {
  EXPECT_STREQ("connected", mqttStateText(0));
  EXPECT_STREQ("timeout", mqttStateText(-4));
  EXPECT_STREQ("connection lost", mqttStateText(-3));
  EXPECT_STREQ("cannot reach broker", mqttStateText(-2));
  EXPECT_STREQ("disconnected", mqttStateText(-1));
  EXPECT_STREQ("protocol rejected", mqttStateText(1));
  EXPECT_STREQ("client id rejected", mqttStateText(2));
  EXPECT_STREQ("broker unavailable", mqttStateText(3));
  EXPECT_STREQ("bad user or password", mqttStateText(4));
  EXPECT_STREQ("not authorized", mqttStateText(5));
  EXPECT_STREQ("unknown", mqttStateText(99));
}

TEST(MqttStatus, FormatStatsShowsEveryCounter) {
  BridgeStats s;
  memset(&s, 0, sizeof(s));
  s.published = 12;
  s.injected_flood = 3;
  s.injected_direct = 1;
  s.dropped_ratelimit = 4;
  s.dropped_queue_full = 5;
  s.dropped_duplicate = 6;
  s.suppressed_publish = 7;
  s.own_dropped = 8;
  s.bad_envelope = 9;
  s.rejected_policy = 10;
  char out[160];
  ASSERT_GT(formatStats(s, out, sizeof(out)), 0);
  EXPECT_STREQ("> pub 12 inj 3f/1d rate 4 q 5 dup 6 sup 7 own 8 bad 9 rej 10", out);
}

TEST(MqttStatus, FormatStatsFitsTheCliReplyEvenWithHugeCounters) {
  BridgeStats s;
  memset(&s, 0xFF, sizeof(s));  // every counter 4294967295
  char out[160];
  int n = formatStats(s, out, sizeof(out));
  ASSERT_GT(n, 0);
  EXPECT_LT(n, 150);
  EXPECT_EQ((size_t)n, strlen(out));
}

TEST(MqttStatus, FormatStatsRefusesAnUndersizedBuffer) {
  BridgeStats s;
  memset(&s, 0, sizeof(s));
  char small[16];
  EXPECT_EQ(0, formatStats(s, small, sizeof(small)));
  EXPECT_EQ(0, formatStats(s, small, 0));
}
