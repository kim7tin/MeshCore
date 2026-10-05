#include <gtest/gtest.h>
#include "helpers/bridges/mqtt/NeighborTable.h"

using namespace mqttbridge;

TEST(NeighborTable, FirstTouchIsFreshRepeatIsNot) {
  NeighborTable t;
  EXPECT_TRUE(t.touch(0x31, 1000));
  EXPECT_FALSE(t.touch(0x31, 2000));
  EXPECT_TRUE(t.isActive(0x31));
  EXPECT_FALSE(t.isActive(0x32));
}

TEST(NeighborTable, ExpiresAfterTtlAndReportsOnce) {
  NeighborTable t(180000);
  t.touch(0x31, 1000);
  uint8_t out[8];
  EXPECT_EQ(0, t.expire(1000 + 179999, out, 8));
  ASSERT_EQ(1, t.expire(1000 + 180000, out, 8));
  EXPECT_EQ(0x31, out[0]);
  EXPECT_FALSE(t.isActive(0x31));
  EXPECT_EQ(0, t.expire(1000 + 180001, out, 8));  // reported only once
  EXPECT_TRUE(t.touch(0x31, 200000));              // subscribes again after expiry
}

TEST(NeighborTable, TouchExtendsLifetime) {
  NeighborTable t(180000);
  t.touch(0x31, 0);
  t.touch(0x31, 100000);
  uint8_t out[8];
  EXPECT_EQ(0, t.expire(180000, out, 8));
  EXPECT_EQ(1, t.expire(280000, out, 8));
}

TEST(NeighborTable, ExpireHonoursCapacityAndKeepsTheRest) {
  NeighborTable t(1000);
  t.touch(0x01, 0);
  t.touch(0x02, 0);
  t.touch(0x03, 0);
  uint8_t out[2];
  EXPECT_EQ(2, t.expire(5000, out, 2));
  uint8_t out2[8];
  EXPECT_EQ(1, t.expire(5000, out2, 8));  // the third one comes out on the next call
}

TEST(NeighborTable, CollectActive) {
  NeighborTable t;
  t.touch(0x40, 0);
  t.touch(0x10, 0);
  uint8_t out[8];
  ASSERT_EQ(2, t.collectActive(out, 8));
  EXPECT_EQ(0x10, out[0]);  // ascending hash order
  EXPECT_EQ(0x40, out[1]);
  EXPECT_EQ(1, t.collectActive(out, 1));  // capped
}

TEST(NeighborTable, SurvivesMillisWrap) {
  NeighborTable t(180000);
  const uint32_t near_wrap = 0xFFFFFF00u;
  t.touch(0x31, near_wrap);
  uint8_t out[8];
  EXPECT_EQ(0, t.expire(near_wrap + 179999u, out, 8));   // wraps past 0, still alive
  EXPECT_EQ(1, t.expire(near_wrap + 180000u, out, 8));
}
