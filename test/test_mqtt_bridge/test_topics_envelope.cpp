#include <gtest/gtest.h>
#include <string.h>
#include "helpers/bridges/mqtt/MqttTopics.h"
#include "helpers/bridges/mqtt/MqttEnvelope.h"

using namespace mqttbridge;

TEST(Topics, BuildsNodeAndGroupTopics) {
  char t[32];
  ASSERT_EQ(8u, makeTopic("ktm", TopicKind::NODE, 0xa7, t, sizeof(t)));
  EXPECT_STREQ("ktm/n/a7", t);
  ASSERT_EQ(8u, makeTopic("ktm", TopicKind::GROUP, 0x11, t, sizeof(t)));
  EXPECT_STREQ("ktm/g/11", t);
  ASSERT_EQ(8u, makeTopic("ktm", TopicKind::NODE, 0x05, t, sizeof(t)));
  EXPECT_STREQ("ktm/n/05", t);
}

TEST(Topics, ReturnsZeroWhenBufferTooSmall) {
  char t[8];
  EXPECT_EQ(0u, makeTopic("ktm", TopicKind::NODE, 0xa7, t, sizeof(t)));  // needs 9 with NUL
}

TEST(Topics, ParseRoundTrip) {
  char t[32];
  for (int h = 0; h < 256; h++) {
    makeTopic("mesh/eu", TopicKind::NODE, (uint8_t)h, t, sizeof(t));
    TopicKind k;
    uint8_t out = 0;
    ASSERT_TRUE(parseTopic("mesh/eu", t, &k, &out)) << t;
    EXPECT_EQ(TopicKind::NODE, k);
    EXPECT_EQ(h, out);
  }
}

TEST(Topics, ParseRejectsMalformed) {
  TopicKind k;
  uint8_t h;
  EXPECT_FALSE(parseTopic("ktm", "ktm/x/11", &k, &h));       // unknown kind
  EXPECT_FALSE(parseTopic("ktm", "other/n/11", &k, &h));     // wrong prefix
  EXPECT_FALSE(parseTopic("ktm", "ktm/n/1", &k, &h));        // too short
  EXPECT_FALSE(parseTopic("ktm", "ktm/n/zz", &k, &h));       // not hex
  EXPECT_FALSE(parseTopic("ktm", "ktm/n/A7", &k, &h));       // upper case never published
  EXPECT_FALSE(parseTopic("ktm", "ktm/n/a7/extra", &k, &h)); // trailing data
  EXPECT_FALSE(parseTopic("ktm", "ktmn/a7", &k, &h));        // missing separator
  EXPECT_FALSE(parseTopic("ktm", "", &k, &h));
}

TEST(Envelope, RoundTrip) {
  const uint8_t rid[4] = {1, 2, 3, 4};
  const uint8_t raw[5] = {0x11, 0x00, 0xAA, 0xBB, 0xCC};
  uint8_t buf[32];
  size_t n = encodeEnvelope(rid, raw, sizeof(raw), buf, sizeof(buf));
  ASSERT_EQ(ENVELOPE_HEADER_LEN + sizeof(raw), n);
  EXPECT_EQ(1, buf[0]);  // version
  EXPECT_EQ(0, buf[1]);  // flags

  uint8_t rid_out[4];
  const uint8_t* raw_out = nullptr;
  size_t raw_len = 0;
  ASSERT_TRUE(decodeEnvelope(buf, n, rid_out, &raw_out, &raw_len));
  EXPECT_EQ(0, memcmp(rid, rid_out, 4));
  ASSERT_EQ(sizeof(raw), raw_len);
  EXPECT_EQ(0, memcmp(raw, raw_out, raw_len));
}

TEST(Envelope, EncodeFailsOnEmptyRawOrSmallBuffer) {
  const uint8_t rid[4] = {0};
  const uint8_t raw[3] = {1, 2, 3};
  uint8_t buf[8];
  EXPECT_EQ(0u, encodeEnvelope(rid, raw, 0, buf, sizeof(buf)));
  EXPECT_EQ(0u, encodeEnvelope(rid, raw, sizeof(raw), buf, 8));  // needs 9
}

TEST(Envelope, DecodeRejectsShortOrWrongVersion) {
  uint8_t rid[4];
  const uint8_t* raw;
  size_t raw_len;
  const uint8_t only_header[6] = {1, 0, 1, 2, 3, 4};      // no raw bytes at all
  EXPECT_FALSE(decodeEnvelope(only_header, sizeof(only_header), rid, &raw, &raw_len));
  EXPECT_FALSE(decodeEnvelope(only_header, 3, rid, &raw, &raw_len));
  EXPECT_FALSE(decodeEnvelope(only_header, 0, rid, &raw, &raw_len));
  const uint8_t v2[8] = {2, 0, 1, 2, 3, 4, 9, 9};
  EXPECT_FALSE(decodeEnvelope(v2, sizeof(v2), rid, &raw, &raw_len));
}
