#include <gtest/gtest.h>
#include <string.h>
#include <deque>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "Packet.h"
#include "helpers/bridges/mqtt/BridgeCore.h"

using namespace mqttbridge;
using mesh::Packet;

static uint32_t zeroRand(uint32_t) { return 0; }

static std::string hexKey(const Packet& p) {
  uint8_t k[8];
  p.calculatePacketHash(k);
  char b[17];
  for (int i = 0; i < 8; i++) snprintf(&b[i * 2], 3, "%02x", k[i]);
  return b;
}

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

static std::vector<uint8_t> body(uint8_t dest, uint8_t src, uint8_t seed) {
  std::vector<uint8_t> v(24);
  for (size_t i = 0; i < v.size(); i++) v[i] = (uint8_t)(seed * 37 + i * 11);
  v[0] = dest;
  v[1] = src;
  return v;
}

class World;

class Node {
public:
  World& w;
  int id;
  Node(World& world, int i) : w(world), id(i) {}
  virtual ~Node() {}
  virtual void rxRF(const Packet& p) = 0;
};

class World {
public:
  std::vector<std::unique_ptr<Node>> nodes;
  std::vector<std::set<int>> rf;  // RF adjacency (symmetric)
  std::deque<std::function<void()>> events;
  uint32_t now = 1000;
  int broker_msgs = 0;
  std::function<void()> flushCores;

  int add(Node* n) {
    nodes.emplace_back(n);
    rf.emplace_back();
    return (int)nodes.size() - 1;
  }
  void link(int a, int b) {
    rf[a].insert(b);
    rf[b].insert(a);
  }
  void broadcastRF(int from, const Packet& p) {
    for (int to : rf[from]) {
      Packet copy = p;
      events.push_back([this, to, copy]() { nodes[to]->rxRF(copy); });
    }
  }
  void settle() {
    do {
      while (!events.empty()) {
        auto e = events.front();
        events.pop_front();
        e();
      }
      flushCores();
    } while (!events.empty());
  }
  void advance(uint32_t ms) {
    now += ms;
    settle();
  }
};

class Endpoint : public Node {
public:
  std::vector<Packet> got;
  Endpoint(World& world, int i) : Node(world, i) {}
  void rxRF(const Packet& p) override { got.push_back(p); }
  void send(const Packet& p) { w.broadcastRF(id, p); }
};

class Relay : public Node, public BridgeSink {
public:
  uint8_t hash;
  uint8_t relay_id[4];
  std::unique_ptr<BridgeCore> core;
  std::set<std::string> subs;
  std::set<std::string> seen;  // models the repeater's SimpleMeshTables
  std::vector<Relay*>* all = nullptr;

  Relay(World& world, int i, uint8_t h) : Node(world, i), hash(h) {
    relay_id[0] = h;
    relay_id[1] = relay_id[2] = relay_id[3] = 0x5E;
    core.reset(new BridgeCore(BridgeConfig::defaults("ktm"), relay_id, *this, zeroRand));
    core->setSelfHash(h);
    subs.insert("ktm/g/11");  // enabled group channels are always subscribed
  }

  static std::string topic(TopicKind k, uint8_t h) {
    char t[32];
    makeTopic("ktm", k, h, t, sizeof(t));
    return t;
  }

  // ---- BridgeSink: the in-memory broker ----
  void subscribe(TopicKind k, uint8_t h) override { subs.insert(topic(k, h)); }
  void unsubscribe(TopicKind k, uint8_t h) override { subs.erase(topic(k, h)); }
  void publish(TopicKind k, uint8_t h, const uint8_t* env, size_t n) override {
    std::string t = topic(k, h);
    std::vector<uint8_t> data(env, env + n);
    w.broker_msgs++;
    for (Relay* r : *all) {
      if (r->subs.count(t)) {
        w.events.push_back([r, t, data]() { r->onMqtt(t, data); });
      }
    }
  }

  void onMqtt(const std::string& t, const std::vector<uint8_t>& data) {
    TopicKind k;
    uint8_t h;
    ASSERT_TRUE(parseTopic("ktm", t.c_str(), &k, &h));
    Inject r = core->onMqtt(k, h, data.data(), data.size(), w.now);
    if (r.kind == InjectKind::NONE) return;
    Packet p;
    ASSERT_TRUE(p.readFrom(r.raw, (uint8_t)r.raw_len));
    if (r.kind == InjectKind::FLOOD_INBOUND) {
      processFlood(p);  // queueInbound -> normal receive path, no logRx
    } else {
      transmit(p);  // queueOutbound -> transmit, logTx still runs
    }
  }

  // ---- Repeater model (Mesh::onRecvPacket) ----
  void rxRF(const Packet& in) override {
    core->onRadioRx(in, w.now);  // logRx: every received packet
    Packet p = in;
    if (p.isRouteDirect()) {  // Mesh.cpp:78-110
      if (p.getPathHashCount() > 0 && p.path[0] == hash && !seen.count(hexKey(p))) {
        seen.insert(hexKey(p));
        uint8_t n = p.getPathHashCount();
        memmove(p.path, p.path + 1, n - 1);  // removeSelfFromPath (1-byte hashes)
        p.setPathHashCount(n - 1);
        transmit(p);
      }
      return;
    }
    processFlood(p);
  }

  void processFlood(Packet p) {  // Mesh.cpp:114-141 and routeRecvPacket :344-357
    std::string k = hexKey(p);
    if (seen.count(k)) return;
    seen.insert(k);
    uint8_t n = p.getPathHashCount();
    p.path[n] = hash;
    p.setPathHashCount((uint8_t)(n + 1));
    transmit(p);
  }

  void transmit(const Packet& p) {
    core->onRadioTx(p, w.now);  // logTx
    w.broadcastRF(id, p);
  }
};

struct Scene {
  World w;
  std::vector<Relay*> relays;
  Endpoint *A, *B;
  Relay *R1, *R2;
  // constexpr (implicitly inline in C++17): the tests pass these by reference to EXPECT_EQ
  static constexpr uint8_t HA = 0xA1, HB = 0xB1, H1 = 0x21, H2 = 0x22;

  Scene() {
    A = new Endpoint(w, 0);
    w.add(A);
    B = new Endpoint(w, 1);
    w.add(B);
    R1 = addRelay(H1);
    R2 = addRelay(H2);
    w.link(A->id, R1->id);  // A hears only R1, B hears only R2; R1 and R2 are out of RF range
    w.link(B->id, R2->id);
    w.flushCores = [this]() {
      for (Relay* r : relays) r->core->loop(w.now);
    };
  }
  Relay* addRelay(uint8_t h) {
    Relay* r = new Relay(w, (int)w.nodes.size(), h);
    w.add(r);
    relays.push_back(r);
    for (Relay* x : relays) x->all = &relays;
    return r;
  }
  void advertBoth() {  // each endpoint's zero-hop advert makes its relay subscribe to it
    std::vector<uint8_t> adA(100, 3), adB(100, 4);
    adA[0] = HA;
    adB[0] = HB;
    A->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, adA));
    B->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_ADVERT, {}, adB));
    w.settle();
  }
};

TEST(BridgeSim, AdvertMakesRelaySubscribeButStaysLocal) {
  Scene s;
  s.advertBoth();
  EXPECT_TRUE(s.R1->subs.count("ktm/n/a1"));
  EXPECT_TRUE(s.R2->subs.count("ktm/n/b1"));
  EXPECT_FALSE(s.R1->subs.count("ktm/n/b1"));
  EXPECT_EQ(0, s.w.broker_msgs);  // adverts are not bridged
  // B only hears R2 forwarding B's own advert, never A's advert (payload[0] is the sender hash)
  for (const Packet& p : s.B->got) EXPECT_EQ(Scene::HB, p.payload[0]);
}

TEST(BridgeSim, FloodTextCrossesTheBridgeWithTheLocalRelayInThePath) {
  Scene s;
  s.advertBoth();
  s.B->got.clear();
  s.A->got.clear();
  s.B->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, body(Scene::HA, Scene::HB, 1)));
  s.w.settle();
  ASSERT_EQ(1u, s.A->got.size());
  const Packet& rx = s.A->got[0];
  EXPECT_EQ(1, rx.getPathHashCount());
  EXPECT_EQ(Scene::H1, rx.path[0]);  // A sees only its own relay
  EXPECT_EQ(1, s.w.broker_msgs);     // R1 never re-published the injected copy
}

TEST(BridgeSim, PathReplyFloodComesBackTheSameWay) {
  Scene s;
  s.advertBoth();
  s.B->got.clear();
  s.A->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_PATH, {}, body(Scene::HB, Scene::HA, 2)));
  s.w.settle();
  ASSERT_EQ(1u, s.B->got.size());
  EXPECT_EQ(Scene::H2, s.B->got[0].path[0]);
}

TEST(BridgeSim, DirectTextAndItsAckCrossTheBridge) {
  Scene s;
  s.advertBoth();
  s.A->got.clear();
  s.B->got.clear();
  // A -> B direct, A's route to B is [R1]
  s.A->send(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_TXT_MSG, {Scene::H1}, body(Scene::HB, Scene::HA, 3)));
  s.w.settle();
  ASSERT_EQ(1u, s.B->got.size());
  EXPECT_EQ(0, s.B->got[0].getPathHashCount());
  // B -> A direct ACK, B's route to A is [R2]; the last hop has no destination in the payload
  s.A->got.clear();  // A also heard R1 forwarding its own message
  s.B->send(mk(ROUTE_TYPE_DIRECT, PAYLOAD_TYPE_ACK, {Scene::H2}, std::vector<uint8_t>(4, 0x77)));
  s.w.settle();
  ASSERT_EQ(1u, s.A->got.size());
  EXPECT_EQ(PAYLOAD_TYPE_ACK, s.A->got[0].getPayloadType());
}

TEST(BridgeSim, PublicGroupMessageCrossesAndIsNotEchoedBack) {
  Scene s;
  s.advertBoth();
  s.B->got.clear();
  std::vector<uint8_t> g(24, 0x31);
  g[0] = 0x11;
  s.A->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_GRP_TXT, {}, g));
  s.w.settle();
  ASSERT_EQ(1u, s.B->got.size());
  EXPECT_EQ(Scene::H2, s.B->got[0].path[0]);
  EXPECT_EQ(1, s.w.broker_msgs);
}

TEST(BridgeSim, SubscriptionExpiresAfterThreeMinutesOfSilence) {
  Scene s;
  s.advertBoth();
  s.w.advance(181000);
  EXPECT_FALSE(s.R1->subs.count("ktm/n/a1"));
  s.A->got.clear();
  s.B->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, body(Scene::HA, Scene::HB, 5)));
  s.w.settle();
  EXPECT_TRUE(s.A->got.empty());  // nobody is listening for A any more
}

TEST(BridgeSim, TwoInjectingRelaysDoNotLoopAndTheDestinationHearsEachOnce) {
  Scene s;
  Relay* R3 = s.addRelay(0x23);
  s.w.link(s.B->id, R3->id);
  s.w.link(s.R2->id, R3->id);  // R2 and R3 can hear each other and both reach B
  s.advertBoth();              // both R2 and R3 hear B and subscribe to it
  s.B->got.clear();
  int before = s.w.broker_msgs;
  s.A->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, body(Scene::HB, Scene::HA, 6)));
  s.w.settle();
  EXPECT_EQ(1, s.w.broker_msgs - before);  // one publish, nothing re-published
  EXPECT_EQ(2u, s.B->got.size());          // one copy per injecting relay, B de-duplicates itself
}

TEST(BridgeSim, WorksAsPlainRepeaterWhenNothingIsSubscribed) {
  Scene s;
  s.A->send(mk(ROUTE_TYPE_FLOOD, PAYLOAD_TYPE_TXT_MSG, {}, body(Scene::HB, Scene::HA, 7)));
  s.w.settle();
  EXPECT_TRUE(s.B->got.empty());     // B is out of range and no bridge topic is active
  EXPECT_FALSE(s.R1->seen.empty());  // but R1 still handled the packet like a repeater
}
