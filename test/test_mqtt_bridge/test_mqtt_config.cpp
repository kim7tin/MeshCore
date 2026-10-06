#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include "helpers/bridges/mqtt/MqttConfig.h"

using namespace mqttbridge;

static bool setOk(MqttConfig& c, const char* k, const char* v) {
  char err[48];
  return c.set(k, v, err, sizeof(err));
}

static std::string getStr(const MqttConfig& c, const char* k) {
  char out[96];
  return c.get(k, out, sizeof(out)) ? std::string(out) : std::string("<unknown>");
}

TEST(MqttConfig, DefaultsAreEmptyButUsable) {
  MqttConfig c;
  EXPECT_FALSE(c.configured());
  EXPECT_EQ("(not set)", getStr(c, "ssid"));
  EXPECT_EQ("1883", getStr(c, "port"));
  EXPECT_EQ("1883", getStr(c, "port2"));
  EXPECT_EQ("on", getStr(c, "group"));
  EXPECT_EQ("meshcore", getStr(c, "prefix"));
}

TEST(MqttConfig, ConfiguredNeedsWifiNameAndFirstBrokerHost) {
  MqttConfig c;
  ASSERT_TRUE(setOk(c, "ssid", "Nha Tro 2"));
  EXPECT_FALSE(c.configured());
  ASSERT_TRUE(setOk(c, "host", "broker.example.org"));
  EXPECT_TRUE(c.configured());
  ASSERT_TRUE(setOk(c, "ssid", ""));  // empty value clears
  EXPECT_FALSE(c.configured());
}

TEST(MqttConfig, SecondBrokerAloneDoesNotMakeItConfigured) {
  MqttConfig c;
  setOk(c, "ssid", "net");
  setOk(c, "host2", "second.example.org");
  EXPECT_FALSE(c.configured());
}

TEST(MqttConfig, ValuesKeepSpacesAndSpecialCharacters) {
  MqttConfig c;
  ASSERT_TRUE(setOk(c, "ssid", "My Net = \"1\"'s"));
  EXPECT_EQ("My Net = \"1\"'s", getStr(c, "ssid"));
  ASSERT_TRUE(setOk(c, "wifipass", "p@ss word=1\\2"));
  ASSERT_TRUE(setOk(c, "pass", "a=b=c"));
}

TEST(MqttConfig, SecretsAreMaskedByGetButClearable) {
  MqttConfig c;
  ASSERT_TRUE(setOk(c, "wifipass", "supersecret1"));
  ASSERT_TRUE(setOk(c, "pass", "mqttsecret"));
  ASSERT_TRUE(setOk(c, "pass2", "other"));
  EXPECT_EQ("***", getStr(c, "wifipass"));
  EXPECT_EQ("***", getStr(c, "pass"));
  EXPECT_EQ("***", getStr(c, "pass2"));
  ASSERT_TRUE(setOk(c, "pass", ""));
  EXPECT_EQ("(not set)", getStr(c, "pass"));
}

TEST(MqttConfig, RejectsBadValuesWithAReason) {
  MqttConfig c;
  char err[48];
  EXPECT_FALSE(c.set("port", "0", err, sizeof(err)));
  EXPECT_NE(std::string(err).size(), 0u);
  EXPECT_FALSE(setOk(c, "port", "65536"));
  EXPECT_FALSE(setOk(c, "port", "abc"));
  EXPECT_FALSE(setOk(c, "port", "18x3"));
  EXPECT_FALSE(setOk(c, "port", "-1"));
  EXPECT_TRUE(setOk(c, "port", "65535"));
  EXPECT_FALSE(setOk(c, "ssid", "123456789012345678901234567890123"));  // 33 chars
  EXPECT_TRUE(setOk(c, "ssid", "12345678901234567890123456789012"));    // 32 chars
  EXPECT_FALSE(setOk(c, "wifipass", std::string(64, 'x').c_str()));
  EXPECT_TRUE(setOk(c, "wifipass", std::string(63, 'x').c_str()));
  EXPECT_FALSE(setOk(c, "host", std::string(64, 'h').c_str()));
  EXPECT_FALSE(setOk(c, "host", "bad host"));
  EXPECT_FALSE(setOk(c, "user", std::string(32, 'u').c_str()));
  EXPECT_FALSE(setOk(c, "pass", std::string(64, 'p').c_str()));
  EXPECT_FALSE(setOk(c, "group", "maybe"));
  EXPECT_TRUE(setOk(c, "group", "off"));
  EXPECT_EQ("off", getStr(c, "group"));
}

TEST(MqttConfig, RejectsControlCharactersEverywhere) {
  MqttConfig c;
  EXPECT_FALSE(setOk(c, "ssid", "a\nb"));
  EXPECT_FALSE(setOk(c, "ssid", "a\rb"));
  EXPECT_FALSE(setOk(c, "pass", "a\tb"));
  EXPECT_FALSE(setOk(c, "user", "a\x01"));
  EXPECT_FALSE(setOk(c, "wifipass", "a\x7f"));
}

TEST(MqttConfig, PrefixMustBeAValidTopicPrefix) {
  MqttConfig c;
  EXPECT_TRUE(setOk(c, "prefix", "ktm"));
  EXPECT_TRUE(setOk(c, "prefix", "mesh/eu-1_a"));
  EXPECT_FALSE(setOk(c, "prefix", ""));          // empty would give topics like "/n/a1"
  EXPECT_FALSE(setOk(c, "prefix", "/ktm"));
  EXPECT_FALSE(setOk(c, "prefix", "ktm/"));
  EXPECT_FALSE(setOk(c, "prefix", "a//b"));
  EXPECT_FALSE(setOk(c, "prefix", "a#"));
  EXPECT_FALSE(setOk(c, "prefix", "a+b"));
  EXPECT_FALSE(setOk(c, "prefix", "a b"));
  EXPECT_FALSE(setOk(c, "prefix", "$SYS"));
  EXPECT_FALSE(setOk(c, "prefix", std::string(21, 'a').c_str()));
  EXPECT_EQ("mesh/eu-1_a", getStr(c, "prefix"));  // rejected values left the last good one in place
}

TEST(MqttConfig, UnknownKeysAreRejected) {
  MqttConfig c;
  char err[48], out[16];
  EXPECT_FALSE(c.set("nope", "1", err, sizeof(err)));
  EXPECT_FALSE(c.get("nope", out, sizeof(out)));
  EXPECT_FALSE(c.set("", "1", err, sizeof(err)));
}

TEST(MqttConfig, GetNeverOverrunsASmallBuffer) {
  MqttConfig c;
  setOk(c, "ssid", "a-fairly-long-network-name");
  char out[8];
  memset(out, 'Z', sizeof(out));
  EXPECT_FALSE(c.get("ssid", out, sizeof(out)));   // does not fit: refused, buffer stays terminated
  EXPECT_LT(strlen(out), sizeof(out));
  char one[1];
  EXPECT_FALSE(c.get("ssid", one, 0));
}

TEST(MqttConfig, SerializeParseRoundTripKeepsSecrets) {
  MqttConfig a;
  setOk(a, "ssid", "Nha Tro 2");
  setOk(a, "wifipass", "wifi=pass");
  setOk(a, "host", "broker.example.org");
  setOk(a, "port", "8883");
  setOk(a, "user", "relay");
  setOk(a, "pass", "s3cret");
  setOk(a, "host2", "b2.example.org");
  setOk(a, "prefix", "ktm");
  setOk(a, "group", "off");
  char buf[512];
  size_t n = a.serialize(buf, sizeof(buf));
  ASSERT_GT(n, 0u);
  ASSERT_LT(n, sizeof(buf));
  EXPECT_NE(nullptr, strstr(buf, "pass=s3cret\n"));  // the file holds the real secrets

  MqttConfig b;
  b.parse(buf);
  char o1[96], o2[96];
  const char* keys[] = {"ssid", "host", "port", "user", "host2", "port2", "prefix", "group"};
  for (const char* k : keys) {
    ASSERT_TRUE(a.get(k, o1, sizeof(o1)));
    ASSERT_TRUE(b.get(k, o2, sizeof(o2)));
    EXPECT_STREQ(o1, o2) << k;
  }
  char again[512];
  b.serialize(again, sizeof(again));
  EXPECT_STREQ(buf, again);  // secrets survived (get() masks them, so compare the files)
}

TEST(MqttConfig, SerializeReportsWhenItDoesNotFit) {
  MqttConfig a;
  setOk(a, "ssid", "network");
  setOk(a, "host", "broker.example.org");
  char small[10];
  EXPECT_EQ(0u, a.serialize(small, sizeof(small)));
  EXPECT_EQ(0u, a.serialize(small, 0));
}

TEST(MqttConfig, ParseIgnoresGarbageAndKeepsGoodLines) {
  MqttConfig c;
  const char* text =
      "garbage line without equals\r\n"
      "ssid=Good Net\r\n"            // CRLF endings from a Windows editor
      "port=notanumber\n"
      "=novalue\n"
      "unknown=1\n"
      "\n"
      "host=broker.example.org\n"
      "pass=" "\x01" "bad\n";         // control char: line ignored
  c.parse(text);
  EXPECT_EQ("Good Net", getStr(c, "ssid"));
  EXPECT_EQ("broker.example.org", getStr(c, "host"));
  EXPECT_EQ("1883", getStr(c, "port"));
  EXPECT_EQ("(not set)", getStr(c, "pass"));
}

TEST(MqttConfig, ParseSurvivesVeryLongLinesAndMissingNewline) {
  MqttConfig c;
  std::string text = "ssid=" + std::string(5000, 'x') + "\nhost=h.example.org";
  c.parse(text.c_str());
  EXPECT_EQ("(not set)", getStr(c, "ssid"));           // too long: rejected, not truncated
  EXPECT_EQ("h.example.org", getStr(c, "host"));       // last line without '\n' still read
  c.parse("");
  c.parse(nullptr);                                    // no crash
}
