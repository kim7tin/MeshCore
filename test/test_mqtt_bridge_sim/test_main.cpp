#include <gtest/gtest.h>

// One main() per PlatformIO test directory; the other files only define tests.
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
