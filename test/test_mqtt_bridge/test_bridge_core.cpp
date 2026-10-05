#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <vector>
#include "Packet.h"
#include "helpers/bridges/mqtt/BridgeCore.h"

using namespace mqttbridge;
using mesh::Packet;

static const uint8_t SELF = 0x21;

static Packet mk(uint8_t route, uint8_t type, const std::vector<uint8_t>& path,
                 const std::vector<uint8_t>& payload) {
  Packet p;
  p.header = route | (type << PH_TYPE_SHIFT);
  p.transport_codes[0] = p.transport_codes[1] = 0;
  p.setPathHashSizeAndCount(1, (uint8_t)path.size());
  memcpy(p.path, path.data(), path.size());
  p.payload_len = (uint16_t)payload.size();
  memcpy(p.payload, payload.data(), payload.size());
  return p;
}

// TXT-like payload: dest, src, then filler that differs per 'seed' so packet keys differ.
static std::vector<uint8_t> txt(uint8_t dest, uint8_t src, uint8_t seed = 1) {
  std::vector<uint8_t> v(24);
  for (size_t i = 0; i < v.size(); i++) v[i] = (uint8_t)(seed * 31 + i * 7);
  v[0] = dest;
  v[1] = src;
  return v;
}

struct Rec {
  std::string op;
  TopicKind kind;
  uint8_t hash;
  std::vector<uint8_t> env;
};

struct RecSink : BridgeSink {
  std::vector<Rec> log;
  void subscribe(TopicKind k, uint8_t h) override { log.push_back({"sub", k, h, {}}); }
  void unsubscribe(TopicKind k, uint8_t h) override { log.push_back({"unsub", k, h, {}}); }
  void publish(TopicKind k, uint8_t h, const uint8_t* e, size_t n) override {
    log.push_back({"pub", k, h, std::vector<uint8_t>(e, e + n)});
  }
  int count(const std::string& op, TopicKind k, uint8_t h) const {
    int n = 0;
    for (const Rec& r : log)
      if (r.op == op && r.kind == k && r.hash == h) n++;
    return n;
  }
  int count(const std::string& op) const {
    int n = 0;
    for (const Rec& r : log)
      if (r.op == op) n++;
    return n;
  }
};

static uint32_t zeroRand(uint32_t) { return 0; }
static uint32_t highRand(uint32_t bound) { return bound - 1; }

static BridgeConfig testCfg() {
  BridgeConfig c = BridgeConfig::defaults("ktm");
  c.publish_jitter_ms = 0;
  return c;
}

static std::vector<uint8_t> envelopeOf(const Packet& p, const uint8_t from[4]) {
  uint8_t raw[256];
  size_t rl = p.writeTo(raw);
  uint8_t buf[300];
  size_t n = encodeEnvelope(from, raw, rl, buf, sizeof(buf));
  return std::vector<uint8_t>(buf, buf + n);
}

class BridgeCoreTest : public ::testing::Test {
protected:
  RecSink sink;
  uint8_t rid[4] = {1, 2, 3, 4};
  const uint8_t other[4] = {9, 9, 9, 9};
  BridgeCore core;
  BridgeCoreTest() : core(testCfg(), rid, sink, zeroRand) { core.setSelfHash(SELF); }

  Inject mqtt(const Packet& p, const uint8_t from[4], uint32_t now,
              TopicKind k = TopicKind::NODE, uint8_t h = 0xA1) {
    static std::vector<uint8_t> keep;
    keep = envelopeOf(p, from);
    return core.onMqtt(k, h, keep.data(), keep.size(), now);
  }
  Packet decodePublished(const Rec& r) {
    uint8_t from[4];
    const uint8_t* raw;
    size_t rl;
    EXPECT_TRUE(decodeEnvelope(r.env.data(), r.env.size(), from, &raw, &rl));
    EXPECT_EQ(0, memcmp(from, rid, 4));
    Packet p;
    EXPECT_TRUE(p.readFrom(raw, (uint8_t)rl));
    return p;
  }
};

// ---- subscribe side -------------------------------------------------------------------------

TEST_F(BridgeCoreTest, FloodRxSubscribesLastPathHashOnceAndExpiresAfterTtl) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x30, 0x31}, txt(0xA1, 0xB1));
  core.onRadioRx(p, 1000);
  core.onRadioRx(p, 5000);
  EXPECT_EQ(1, sink.count("sub", TopicKind::NODE, 0x31));
  core.loop(5000 + 179999);
  EXPECT_EQ(0, sink.count("unsub"));
  core.loop(5000 + 180000);
  EXPECT_EQ(1, sink.count("unsub", TopicKind::NODE, 0x31));
}

TEST_F(BridgeCoreTest, EmptyPathFloodUsesSenderAndGroupOrDirectDoNotSubscribe) {
  core.onRadioRx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1)), 0);
  EXPECT_EQ(1, sink.count("sub", TopicKind::NODE, 0xB1));
  core.onRadioRx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, std::vector<uint8_t>(20, 0x11)), 0);
  core.onRadioRx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(1, 2)), 0);
  EXPECT_EQ(1, sink.count("sub"));
}

TEST_F(BridgeCoreTest, ActiveNeighborsAndGroupChannelsAreExposedForResubscribe) {
  core.onRadioRx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(1, 2)), 0);
  uint8_t out[8];
  ASSERT_EQ(1, core.activeNeighbors(out, 8));
  EXPECT_EQ(0x31, out[0]);
  ASSERT_EQ(1, core.groupChannels(out, 8));
  EXPECT_EQ(0x11, out[0]);
}

// ---- publish side ---------------------------------------------------------------------------

TEST_F(BridgeCoreTest, ForwardedFloodIsPublishedWithoutOwnHashToDestTopic) {
  Packet tx = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31, SELF}, txt(0xA1, 0xB1));
  core.onRadioTx(tx, 100);
  core.loop(100);
  ASSERT_EQ(1, sink.count("pub", TopicKind::NODE, 0xA1));
  Packet got = decodePublished(sink.log.back());
  EXPECT_EQ(1, got.getPathHashCount());
  EXPECT_EQ(0x31, got.path[0]);          // our own hash was stripped again
  EXPECT_EQ(tx.payload_len, got.payload_len);
  EXPECT_EQ(1u, core.stats().published);
}

TEST_F(BridgeCoreTest, OriginatedAdvertOrForeignLastHopFloodIsNotPublished) {
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1)), 0);              // originated here
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 2)), 0);        // last hop is not us
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {SELF}, std::vector<uint8_t>(100, 7)), 0);  // no destination
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ACK, {SELF}, std::vector<uint8_t>(4, 5)), 0);
  core.loop(0);
  EXPECT_EQ(0, sink.count("pub"));
}

TEST_F(BridgeCoreTest, GroupPublishOnlyEnabledChannelAndRateLimitedPerMinute) {
  for (int i = 0; i < 7; i++) {
    std::vector<uint8_t> pl(20, (uint8_t)(0x40 + i));
    pl[0] = 0x11;
    core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {SELF}, pl), 1000 + i);
  }
  core.loop(2000);
  EXPECT_EQ(6, sink.count("pub", TopicKind::GROUP, 0x11));  // 7th dropped
  EXPECT_EQ(1u, core.stats().dropped_ratelimit);

  std::vector<uint8_t> other(20, 0x77);
  other[0] = 0x22;  // channel not enabled
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {SELF}, other), 2000);
  core.loop(2000);
  EXPECT_EQ(0, sink.count("pub", TopicKind::GROUP, 0x22));

  std::vector<uint8_t> later(20, 0x66);
  later[0] = 0x11;
  core.onRadioTx(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {SELF}, later), 1000 + 60000);
  core.loop(1000 + 60000);
  EXPECT_EQ(7, sink.count("pub", TopicKind::GROUP, 0x11));  // new window
}

TEST_F(BridgeCoreTest, DirectPacketsPublishToNextHopThenDestination) {
  core.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x55}, txt(0xB1, 0xA1)), 0);
  core.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xB2, 0xA1, 2)), 0);
  core.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TRACE, {0x55}, std::vector<uint8_t>(10, 3)), 0);
  core.loop(0);
  EXPECT_EQ(1, sink.count("pub", TopicKind::NODE, 0x55));
  EXPECT_EQ(1, sink.count("pub", TopicKind::NODE, 0xB2));
  EXPECT_EQ(2, sink.count("pub"));  // TRACE ignored
}

TEST_F(BridgeCoreTest, DirectAckAtLastHopGoesToReplyCacheTopics) {
  // A message from node 0xA1 arrives over MQTT; its ACK will come back toward 0xA1.
  Inject r = mqtt(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xB1, 0xA1)), other, 1000,
                  TopicKind::NODE, 0xB1);
  ASSERT_EQ(InjectKind::DIRECT_OUTBOUND, r.kind);
  core.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, 9)), 2000);
  core.loop(2000);
  EXPECT_EQ(1, sink.count("pub", TopicKind::NODE, 0xA1));
  // 15 s later the cache is empty and the ACK goes nowhere
  core.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, 8)), 1000 + 15000);
  core.loop(1000 + 15000);
  EXPECT_EQ(1, sink.count("pub"));
}

TEST_F(BridgeCoreTest, PublishJitterDelaysAndMqttCopyCancelsPending) {
  BridgeConfig c = testCfg();
  c.publish_jitter_ms = 1000;
  RecSink s2;
  BridgeCore jc(c, rid, s2, highRand);  // always the full 1000 ms
  jc.setSelfHash(SELF);
  Packet tx = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {SELF}, txt(0xA1, 0xB1));
  jc.onRadioTx(tx, 0);
  jc.loop(999);
  EXPECT_EQ(0, s2.count("pub"));
  jc.loop(1000);
  EXPECT_EQ(1, s2.count("pub"));

  // second packet: another relay publishes the same packet first -> ours is cancelled
  Packet tx2 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {SELF}, txt(0xA1, 0xB1, 2));
  jc.onRadioTx(tx2, 5000);
  Packet raw2 = tx2;
  raw2.setPathHashCount(0);  // as the other relay would publish it
  std::vector<uint8_t> env = envelopeOf(raw2, other);
  jc.onMqtt(TopicKind::NODE, 0xA1, env.data(), env.size(), 5200);
  jc.loop(6000);
  EXPECT_EQ(1, s2.count("pub"));
  EXPECT_EQ(1u, jc.stats().suppressed_publish);
}

TEST_F(BridgeCoreTest, QueueFullDropsWithCounterAndDoesNotCrash) {
  BridgeConfig c = testCfg();
  c.publish_jitter_ms = 5000;
  c.pub_per_min = 1000;
  RecSink s2;
  BridgeCore jc(c, rid, s2, highRand);
  jc.setSelfHash(SELF);
  for (int i = 0; i < 8; i++) {  // 8 distinct sources -> ReplyCache holds 8 hashes
    uint8_t src = (uint8_t)(0x60 + i);
    std::vector<uint8_t> env = envelopeOf(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xB1, src, (uint8_t)i)), other);
    ASSERT_EQ(InjectKind::DIRECT_OUTBOUND, jc.onMqtt(TopicKind::NODE, 0xB1, env.data(), env.size(), 100).kind);
  }
  for (int i = 0; i < 3; i++)  // 3 ACKs x 8 topics = 24 pushes into 16 slots
    jc.onRadioTx(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, (uint8_t)(i + 1))), 200);
  EXPECT_EQ(8u, jc.stats().dropped_queue_full);
  jc.loop(200 + 5000);
  EXPECT_EQ(16, s2.count("pub"));
}

// ---- inject side ----------------------------------------------------------------------------

TEST_F(BridgeCoreTest, FloodFromOtherRelayIsInjectedAsInbound) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1));
  Inject r = mqtt(p, other, 1000);
  ASSERT_EQ(InjectKind::FLOOD_INBOUND, r.kind);
  Packet got;
  ASSERT_TRUE(got.readFrom(r.raw, (uint8_t)r.raw_len));
  EXPECT_EQ(p.payload_len, got.payload_len);
  EXPECT_EQ(1, got.getPathHashCount());
  EXPECT_EQ(1u, core.stats().injected_flood);
}

TEST_F(BridgeCoreTest, DirectFromOtherRelayIsInjectedAsOutboundUnlessTrace) {
  EXPECT_EQ(InjectKind::DIRECT_OUTBOUND,
            mqtt(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1)), other, 0).kind);
  EXPECT_EQ(InjectKind::NONE,
            mqtt(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TRACE, {0x31}, std::vector<uint8_t>(10, 1)), other, 0).kind);
  EXPECT_EQ(1u, core.stats().injected_direct);
}

TEST_F(BridgeCoreTest, OwnRelayIdBadEnvelopeAndGarbageAreDropped) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1));
  EXPECT_EQ(InjectKind::NONE, mqtt(p, rid, 0).kind);
  EXPECT_EQ(1u, core.stats().own_dropped);

  const uint8_t garbage[3] = {1, 2, 3};
  EXPECT_EQ(InjectKind::NONE, core.onMqtt(TopicKind::NODE, 1, garbage, sizeof(garbage), 0).kind);
  EXPECT_EQ(InjectKind::NONE, core.onMqtt(TopicKind::NODE, 1, garbage, 0, 0).kind);
  const uint8_t bad_raw[8] = {1, 0, 9, 9, 9, 9, 0xFF, 0xFF};  // valid envelope, unparsable packet
  EXPECT_EQ(InjectKind::NONE, core.onMqtt(TopicKind::NODE, 1, bad_raw, sizeof(bad_raw), 0).kind);
  EXPECT_EQ(3u, core.stats().bad_envelope);
}

TEST_F(BridgeCoreTest, SamePacketFromTwoBrokersOrRelaysIsInjectedOnce) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1));
  const uint8_t third[4] = {7, 7, 7, 7};
  EXPECT_EQ(InjectKind::FLOOD_INBOUND, mqtt(p, other, 1000).kind);
  EXPECT_EQ(InjectKind::NONE, mqtt(p, other, 1001).kind);   // same copy over a second broker
  EXPECT_EQ(InjectKind::NONE, mqtt(p, third, 1002).kind);   // same packet from another relay
  EXPECT_EQ(2u, core.stats().dropped_duplicate);
}

TEST_F(BridgeCoreTest, PacketWeAlreadyTransmittedIsNotInjectedAgain) {
  Packet tx = mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xB1, 0xA1));
  core.onRadioTx(tx, 0);
  EXPECT_EQ(InjectKind::NONE, mqtt(tx, other, 10, TopicKind::NODE, 0xB1).kind);
}

TEST_F(BridgeCoreTest, InjectedPacketIsNeverRepublishedWhenTheRepeaterForwardsIt) {
  Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1));
  ASSERT_EQ(InjectKind::FLOOD_INBOUND, mqtt(p, other, 0).kind);
  Packet fwd = p;                         // the repeater core appends our hash and transmits
  fwd.path[1] = SELF;
  fwd.setPathHashCount(2);
  core.onRadioTx(fwd, 10);
  core.loop(10);
  EXPECT_EQ(0, sink.count("pub"));
}

TEST_F(BridgeCoreTest, InjectRateLimitedPerMinuteAndGroupSeparately) {
  BridgeConfig c = testCfg();
  c.inject_per_min = 2;
  RecSink s2;
  BridgeCore lc(c, rid, s2, zeroRand);
  for (int i = 0; i < 3; i++) {
    std::vector<uint8_t> env = envelopeOf(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, txt(0xA1, 0xB1, (uint8_t)(i + 1))), other);
    Inject r = lc.onMqtt(TopicKind::NODE, 0xA1, env.data(), env.size(), 100);
    EXPECT_EQ(i < 2 ? InjectKind::FLOOD_INBOUND : InjectKind::NONE, r.kind) << i;
  }
  EXPECT_EQ(1u, lc.stats().dropped_ratelimit);
  std::vector<uint8_t> g(20, 0x31);
  g[0] = 0x11;
  std::vector<uint8_t> env = envelopeOf(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, g), other);
  EXPECT_EQ(InjectKind::FLOOD_INBOUND, lc.onMqtt(TopicKind::GROUP, 0x11, env.data(), env.size(), 100).kind);
}

// ---- review fixes ---------------------------------------------------------------------------

TEST_F(BridgeCoreTest, PacketHeardOnRadioIsNotInjectedAndDoesNotSpendTheLimit) {
  BridgeConfig c = testCfg();
  c.inject_per_min = 1;
  RecSink s2;
  BridgeCore lc(c, rid, s2, zeroRand);
  // another relay in our area forwarded this direct packet and we heard it on the radio
  Packet heard = mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x55}, txt(0xB1, 0xA1));
  lc.onRadioRx(heard, 100);
  std::vector<uint8_t> env = envelopeOf(heard, other);
  EXPECT_EQ(InjectKind::NONE, lc.onMqtt(TopicKind::NODE, 0x55, env.data(), env.size(), 200).kind);
  EXPECT_EQ(1u, lc.stats().dropped_duplicate);
  // the one token of the minute is still available for a different packet
  Packet fresh = mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {0x55}, txt(0xB1, 0xA1, 2));
  env = envelopeOf(fresh, other);
  EXPECT_EQ(InjectKind::DIRECT_OUTBOUND, lc.onMqtt(TopicKind::NODE, 0x55, env.data(), env.size(), 300).kind);
}

TEST_F(BridgeCoreTest, RateLimitedMqttPacketIsStillNeverRepublished) {
  BridgeConfig c = testCfg();
  c.inject_per_min = 1;
  RecSink s2;
  BridgeCore lc(c, rid, s2, zeroRand);
  lc.setSelfHash(SELF);
  Packet p1 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 1));
  Packet p2 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 2));
  std::vector<uint8_t> e1 = envelopeOf(p1, other), e2 = envelopeOf(p2, other);
  EXPECT_EQ(InjectKind::FLOOD_INBOUND, lc.onMqtt(TopicKind::NODE, 0xA1, e1.data(), e1.size(), 100).kind);
  EXPECT_EQ(InjectKind::NONE, lc.onMqtt(TopicKind::NODE, 0xA1, e2.data(), e2.size(), 101).kind);  // limited
  // the same packet later reaches us over the radio and the repeater forwards it
  Packet fwd = p2;
  fwd.path[1] = SELF;
  fwd.setPathHashCount(2);
  lc.onRadioTx(fwd, 200);
  lc.loop(200);
  EXPECT_EQ(0, s2.count("pub"));
}

TEST_F(BridgeCoreTest, InjectedKeysAreRememberedAcrossManyPackets) {
  Packet first = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 1));
  ASSERT_EQ(InjectKind::FLOOD_INBOUND, mqtt(first, other, 0).kind);
  for (int i = 2; i <= 40; i++) {
    Packet p = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, (uint8_t)i));
    ASSERT_EQ(InjectKind::FLOOD_INBOUND, mqtt(p, other, 0).kind) << i;
  }
  EXPECT_EQ(InjectKind::NONE, mqtt(first, other, 1).kind);  // 40 packets later: still a duplicate
}

TEST_F(BridgeCoreTest, OnlyBridgedTypesVersionOneAndMatchingTopicsAreInjected) {
  std::vector<uint8_t> grp(24, 0x31);
  grp[0] = 0x11;
  // not a type the publisher emits
  EXPECT_EQ(InjectKind::NONE, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, std::vector<uint8_t>(100, 3)), other, 0).kind);
  EXPECT_EQ(InjectKind::NONE, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_DATA, {}, grp), other, 0, TopicKind::GROUP, 0x11).kind);
  // payload version above 1 is rejected by the repeater too
  Packet v2 = mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 9));
  v2.header |= (2 << PH_VER_SHIFT);
  EXPECT_EQ(InjectKind::NONE, mqtt(v2, other, 0).kind);
  // topic does not match the packet's destination / channel
  EXPECT_EQ(InjectKind::NONE, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {0x31}, txt(0xA1, 0xB1, 10)), other, 0, TopicKind::NODE, 0xB2).kind);
  EXPECT_EQ(InjectKind::NONE, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, grp), other, 0, TopicKind::GROUP, 0x12).kind);
  // a channel that is not enabled
  std::vector<uint8_t> other_ch(24, 0x32);
  other_ch[0] = 0x22;
  EXPECT_EQ(InjectKind::NONE, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, other_ch), other, 0, TopicKind::GROUP, 0x22).kind);
  EXPECT_EQ(6u, core.stats().rejected_policy);
  // what the publisher does emit is accepted: a direct ACK on a node topic (it has no destination)
  EXPECT_EQ(InjectKind::DIRECT_OUTBOUND, mqtt(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {}, std::vector<uint8_t>(4, 0x55)), other, 0, TopicKind::NODE, 0xA1).kind);
  EXPECT_EQ(InjectKind::FLOOD_INBOUND, mqtt(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, grp), other, 0, TopicKind::GROUP, 0x11).kind);
}
