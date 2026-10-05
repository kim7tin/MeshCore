#include <gtest/gtest.h>
#include <string.h>
#include <vector>
#include "Packet.h"
#include "helpers/bridges/mqtt/PacketRules.h"

using namespace mqttbridge;
using mesh::Packet;

static Packet mk(uint8_t route, uint8_t type, const std::vector<uint8_t>& path,
                 const std::vector<uint8_t>& payload, uint8_t hash_size = 1) {
  Packet p;
  p.header = route | (type << PH_TYPE_SHIFT);
  p.transport_codes[0] = p.transport_codes[1] = 0;
  p.setPathHashSizeAndCount(hash_size, (uint8_t)(path.size() / hash_size));
  memcpy(p.path, path.data(), path.size());
  p.payload_len = (uint16_t)payload.size();
  memcpy(p.payload, payload.data(), payload.size());
  return p;
}

static std::vector<uint8_t> txt(uint8_t dest, uint8_t src) {
  std::vector<uint8_t> v(24, 0x5A);
  v[0] = dest;
  v[1] = src;
  return v;
}

TEST(NearbyHash, FloodWithPathUsesLastHash) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31, 0x32, 0x33}, txt(0xA1, 0xB1));
  uint8_t h = 0;
  ASSERT_TRUE(nearbyHash(p, &h));
  EXPECT_EQ(0x33, h);
}

TEST(NearbyHash, TwoAndThreeByteHashesUseFirstByteOfLastHash) {
  Packet p2 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0xAA, 0xBB, 0xCC, 0xDD}, txt(1, 2), 2);
  Packet p3 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG,
                 {0x01, 0x02, 0x03, 0x04, 0x05, 0x06}, txt(1, 2), 3);
  uint8_t h = 0;
  ASSERT_TRUE(nearbyHash(p2, &h));
  EXPECT_EQ(0xCC, h);   // second hash is CC DD; topic uses its first byte
  ASSERT_TRUE(nearbyHash(p3, &h));
  EXPECT_EQ(0x04, h);   // second hash is 04 05 06
}

TEST(NearbyHash, EmptyPathUsesSenderPerPayloadType) {
  uint8_t h = 0;
  Packet t = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1));
  ASSERT_TRUE(nearbyHash(t, &h));
  EXPECT_EQ(0xB1, h);

  std::vector<uint8_t> advert(100, 0);
  advert[0] = 0x44;  // first byte of the public key
  Packet a = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, advert);
  ASSERT_TRUE(nearbyHash(a, &h));
  EXPECT_EQ(0x44, h);

  std::vector<uint8_t> anon(60, 0);
  anon[0] = 0xA1;   // dest
  anon[1] = 0x66;   // first byte of the sender public key
  Packet r = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ANON_REQ, {}, anon);
  ASSERT_TRUE(nearbyHash(r, &h));
  EXPECT_EQ(0x66, h);
}

TEST(NearbyHash, NoSenderForGroupAndAckWithEmptyPath) {
  uint8_t h = 0;
  Packet g = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, std::vector<uint8_t>(20, 0x11));
  Packet k = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, 0x22));
  EXPECT_FALSE(nearbyHash(g, &h));
  EXPECT_FALSE(nearbyHash(k, &h));
}

TEST(NearbyHash, DirectPacketsNeverCount) {
  uint8_t h = 0;
  Packet d = mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(1, 2));
  EXPECT_FALSE(nearbyHash(d, &h));
}

TEST(NearbyHash, ReservedHashesAreIgnored) {
  uint8_t h = 0;
  Packet a = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x00}, txt(1, 2));
  Packet b = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0xFF}, txt(1, 2));
  Packet c = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(1, 0xFF));
  EXPECT_FALSE(nearbyHash(a, &h));
  EXPECT_FALSE(nearbyHash(b, &h));
  EXPECT_FALSE(nearbyHash(c, &h));
}

TEST(NearbyHash, TruncatedPayloadsNeverReadOutOfBounds) {
  uint8_t h = 0;
  Packet t0 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, {});
  Packet t1 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, {0xA1});
  Packet an = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ANON_REQ, {}, std::vector<uint8_t>(32, 1));
  Packet ad = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, std::vector<uint8_t>(31, 1));
  EXPECT_FALSE(nearbyHash(t0, &h));
  EXPECT_FALSE(nearbyHash(t1, &h));
  EXPECT_FALSE(nearbyHash(an, &h));   // needs 1 + 32 bytes
  EXPECT_FALSE(nearbyHash(ad, &h));   // needs 32 bytes
}

TEST(WellFormedRaw, AcceptsWhatWriteToProduces) {
  uint8_t raw[256];
  Packet f = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31, 0x32}, txt(1, 2));
  EXPECT_TRUE(isWellFormedRaw(raw, f.writeTo(raw)));
  Packet t = mk(ROUTE_TYPE_TRANSPORT_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(1, 2));
  t.transport_codes[0] = 0x1234;
  t.transport_codes[1] = 0x5678;
  EXPECT_TRUE(isWellFormedRaw(raw, t.writeTo(raw)));
  Packet h2 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {1, 2, 3, 4}, txt(1, 2), 2);
  EXPECT_TRUE(isWellFormedRaw(raw, h2.writeTo(raw)));
}

TEST(WellFormedRaw, RejectsTruncatedAndInvalidInput) {
  uint8_t raw[300] = {0};
  EXPECT_FALSE(isWellFormedRaw(raw, 0));
  EXPECT_FALSE(isWellFormedRaw(raw, 1));              // header only
  raw[0] = ROUTE_TYPE_TRANSPORT_FLOOD;                // needs 4 bytes of transport codes
  EXPECT_FALSE(isWellFormedRaw(raw, 2));
  EXPECT_FALSE(isWellFormedRaw(raw, 5));              // no path_len byte
  raw[0] = ROUTE_TYPE_FLOOD;
  raw[1] = 5;                                         // 5 one-byte hashes announced
  EXPECT_FALSE(isWellFormedRaw(raw, 6));              // path longer than the data
  EXPECT_FALSE(isWellFormedRaw(raw, 7));              // path fits but there is no payload byte
  EXPECT_TRUE(isWellFormedRaw(raw, 8));               // 1 payload byte
  raw[1] = 0xC0 | 1;                                  // hash size code 3 (4 bytes) is reserved
  EXPECT_FALSE(isWellFormedRaw(raw, 20));
  raw[1] = 0;
  EXPECT_FALSE(isWellFormedRaw(raw, 3 + MAX_PACKET_PAYLOAD));  // payload one byte too big
  EXPECT_TRUE(isWellFormedRaw(raw, 2 + MAX_PACKET_PAYLOAD));
}

TEST(FloodTarget, DestHashTypesGoToNodeTopic) {
  const uint8_t types[] = {PAYLOAD_TYPE_TXT_MSG, PAYLOAD_TYPE_REQ, PAYLOAD_TYPE_RESPONSE,
                           PAYLOAD_TYPE_PATH, PAYLOAD_TYPE_ANON_REQ};
  for (uint8_t ty : types) {
    std::vector<uint8_t> pl(40, 0x5A);
    pl[0] = 0xA1;
    Target t = floodTarget(mk(ROUTE_TYPE_FLOOD, ty, {0x31}, pl));
    EXPECT_EQ(Target::NODE, t.kind) << (int)ty;
    EXPECT_EQ(0xA1, t.hash);
  }
}

TEST(FloodTarget, GroupTextGoesToGroupTopicOthersNone) {
  std::vector<uint8_t> pl(20, 0x5A);
  pl[0] = 0x11;
  Target g = floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {0x31}, pl));
  EXPECT_EQ(Target::GROUP, g.kind);
  EXPECT_EQ(0x11, g.hash);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_DATA, {0x31}, pl)).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, std::vector<uint8_t>(100, 1))).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ACK, {0x31}, std::vector<uint8_t>(4, 1))).kind);
}

TEST(FloodTarget, ReservedDestAndEmptyPayloadAreNone) {
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0x00, 1))).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xFF, 1))).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, {})).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, {})).kind);
  EXPECT_EQ(Target::NONE, floodTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(1, 2))).kind);
}

TEST(DirectTarget, NextHopInPathWins) {
  Target t = directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x55, 0x66}, txt(0xB1, 0xA1)));
  EXPECT_EQ(Target::NODE, t.kind);
  EXPECT_EQ(0x55, t.hash);
}

TEST(DirectTarget, TwoByteHashUsesFirstByteOfNextHash) {
  Target t = directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x55, 0x66}, txt(0xB1, 0xA1), 2));
  EXPECT_EQ(Target::NODE, t.kind);
  EXPECT_EQ(0x55, t.hash);
}

TEST(DirectTarget, EmptyPathUsesDestHashInPayload) {
  Target t = directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xB1, 0xA1)));
  EXPECT_EQ(Target::NODE, t.kind);
  EXPECT_EQ(0xB1, t.hash);
}

TEST(DirectTarget, EmptyPathAckUsesReplyCache) {
  Target t = directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, 7)));
  EXPECT_EQ(Target::REPLY_CACHE, t.kind);
}

TEST(DirectTarget, TraceControlMultipartAreNotBridged) {
  const uint8_t types[] = {PAYLOAD_TYPE_TRACE, PAYLOAD_TYPE_CONTROL, PAYLOAD_TYPE_MULTIPART};
  for (uint8_t ty : types) {
    Packet p = mk(ROUTE_TYPE_DIRECT, ty, {0x55}, std::vector<uint8_t>(10, 1));
    EXPECT_EQ(Target::NONE, directTarget(p).kind) << (int)ty;
    EXPECT_FALSE(isBridgeableDirect(p));
  }
  EXPECT_TRUE(isBridgeableDirect(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(1, 2))));
}

TEST(DirectTarget, ReservedNextHopAndEmptyPayloadAreNone) {
  EXPECT_EQ(Target::NONE, directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0xFF}, txt(1, 2))).kind);
  EXPECT_EQ(Target::NONE, directTarget(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, {})).kind);
}
