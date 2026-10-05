#include <gtest/gtest.h>
#include <string.h>
#include "helpers/bridges/mqtt/KeyRing.h"
#include "helpers/bridges/mqtt/ReplyCache.h"
#include "helpers/bridges/mqtt/WindowLimiter.h"
#include "helpers/bridges/mqtt/Backoff.h"
#include "helpers/bridges/mqtt/PublishQueue.h"

using namespace mqttbridge;

static void keyOf(int i, uint8_t key[8]) {
  memset(key, 0, 8);
  key[0] = (uint8_t)i;
  key[1] = (uint8_t)(i >> 8);
}

TEST(KeyRing, ContainsAfterAddAndEvictsOldest) {
  KeyRing r;
  uint8_t k[8];
  keyOf(1, k);
  EXPECT_FALSE(r.contains(k));
  r.add(k);
  EXPECT_TRUE(r.contains(k));
  for (int i = 2; i <= 33; i++) {  // 32 more distinct keys push key 1 out of 32 slots
    keyOf(i, k);
    r.add(k);
  }
  keyOf(1, k);
  EXPECT_FALSE(r.contains(k));
  keyOf(33, k);
  EXPECT_TRUE(r.contains(k));
}

TEST(KeyRing, AddingTheSameKeyTwiceUsesOneSlot) {
  KeyRing r;
  uint8_t k[8];
  keyOf(7, k);
  for (int i = 0; i < 100; i++) r.add(k);
  for (int i = 100; i < 131; i++) {  // 31 others: key 7 must still be there
    uint8_t o[8];
    keyOf(i, o);
    r.add(o);
  }
  EXPECT_TRUE(r.contains(k));
}

TEST(ReplyCache, CollectsUnexpiredUniqueHashes) {
  ReplyCache c(15000);
  c.note(0xA1, 1000);
  c.note(0xA1, 2000);  // refresh, not a second entry
  c.note(0xB2, 3000);
  uint8_t out[8];
  EXPECT_EQ(2, c.collect(4000, out, 8));
  EXPECT_EQ(1, c.collect(2000 + 15000, out, 8));  // 0xA1 expired at 17000, 0xB2 lives to 18000
  EXPECT_EQ(0xB2, out[0]);
  EXPECT_EQ(0, c.collect(3000 + 15000, out, 8));
}

TEST(ReplyCache, OverwritesOldestBeyondEightEntries) {
  ReplyCache c(15000);
  for (int i = 0; i < 10; i++) c.note((uint8_t)(0x10 + i), 1000);
  uint8_t out[16];
  EXPECT_EQ(8, c.collect(1001, out, 16));
}

TEST(WindowLimiter, AllowsMaxPerWindowThenResets) {
  WindowLimiter l(3, 60000);
  EXPECT_TRUE(l.allow(0));
  EXPECT_TRUE(l.allow(10));
  EXPECT_TRUE(l.allow(20));
  EXPECT_FALSE(l.allow(30));
  EXPECT_FALSE(l.allow(59999));
  EXPECT_TRUE(l.allow(60000));  // new window
}

TEST(WindowLimiter, ZeroMaxBlocksEverythingAndWrapIsSafe) {
  WindowLimiter blocked(0, 1000);
  EXPECT_FALSE(blocked.allow(0));
  WindowLimiter l(1, 1000);
  EXPECT_TRUE(l.allow(0xFFFFFF00u));
  EXPECT_FALSE(l.allow(0xFFFFFF00u + 500));
  EXPECT_TRUE(l.allow(0xFFFFFF00u + 1000));  // wraps past zero
}

TEST(Backoff, FirstAttemptImmediateThenDoublesToCap) {
  Backoff b(1000, 60000);
  EXPECT_TRUE(b.due(0));
  uint32_t now = 0;
  const uint32_t expect[] = {1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000};
  for (uint32_t e : expect) {
    b.fail(now);
    EXPECT_FALSE(b.due(now + e - 1));
    EXPECT_TRUE(b.due(now + e));
    now += e;
  }
}

TEST(Backoff, SuccessResets) {
  Backoff b(1000, 60000);
  b.fail(0);
  b.fail(1000);
  b.success();
  EXPECT_TRUE(b.due(1001));
  b.fail(5000);
  EXPECT_FALSE(b.due(5999));
  EXPECT_TRUE(b.due(6000));  // back to the minimum step
}

TEST(Backoff, DueSurvivesMillisWrap) {
  Backoff b(1000, 60000);
  b.fail(0xFFFFFF00u);
  EXPECT_FALSE(b.due(0xFFFFFF00u + 999u));
  EXPECT_TRUE(b.due(0xFFFFFF00u + 1000u));
}

TEST(PublishQueue, PushPopDueAndCancel) {
  PublishQueue q;
  uint8_t key[8], other[8];
  keyOf(1, key);
  keyOf(2, other);
  const uint8_t raw[3] = {1, 2, 3};
  ASSERT_TRUE(q.push(TopicKind::NODE, 0xA1, key, raw, sizeof(raw), 500));
  ASSERT_TRUE(q.push(TopicKind::GROUP, 0x11, other, raw, sizeof(raw), 100));
  PendingPublish p;
  EXPECT_FALSE(q.popDue(50, &p));
  ASSERT_TRUE(q.popDue(100, &p));
  EXPECT_EQ(TopicKind::GROUP, p.kind);
  EXPECT_EQ(0x11, p.hash);
  EXPECT_EQ(3, p.len);
  EXPECT_EQ(1, q.cancel(key));
  EXPECT_EQ(0, q.cancel(key));
  EXPECT_FALSE(q.popDue(1000, &p));
}

TEST(PublishQueue, RejectsEmptyOversizedAndFull) {
  PublishQueue q;
  uint8_t key[8];
  keyOf(1, key);
  uint8_t big[256] = {0};
  EXPECT_FALSE(q.push(TopicKind::NODE, 1, key, big, 0, 0));
  EXPECT_FALSE(q.push(TopicKind::NODE, 1, key, big, 256, 0));
  for (int i = 0; i < 16; i++) ASSERT_TRUE(q.push(TopicKind::NODE, 1, key, big, 10, 0));
  EXPECT_FALSE(q.push(TopicKind::NODE, 1, key, big, 10, 0));  // 17th: full, no crash
  EXPECT_EQ(16, q.cancel(key));
}

TEST(PublishQueue, DueUsesWrapSafeCompare) {
  PublishQueue q;
  uint8_t key[8];
  keyOf(1, key);
  const uint8_t raw[1] = {9};
  q.push(TopicKind::NODE, 1, key, raw, 1, 0xFFFFFF00u + 300u);  // due after the wrap
  PendingPublish p;
  EXPECT_FALSE(q.popDue(0xFFFFFF00u + 299u, &p));
  EXPECT_TRUE(q.popDue(0xFFFFFF00u + 300u, &p));
}
