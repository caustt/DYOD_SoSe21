#include <limits>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/value_segment.hpp"

namespace opossum {

class StorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<int> int_value_segment;
  ValueSegment<std::string> string_value_segment;
  ValueSegment<double> double_value_segment;
};

TEST_F(StorageValueSegmentTest, GetSize) {
  EXPECT_EQ(int_value_segment.size(), 0u);
  EXPECT_EQ(string_value_segment.size(), 0u);
  EXPECT_EQ(double_value_segment.size(), 0u);
}

TEST_F(StorageValueSegmentTest, AddValueOfSameType) {
  int_value_segment.append(3);
  EXPECT_EQ(type_cast<int>(int_value_segment[0]), 3);
  EXPECT_EQ(int_value_segment.size(), 1u);

  string_value_segment.append("Hello");
  EXPECT_EQ(string_value_segment.size(), 1u);
  EXPECT_EQ(type_cast<std::string>(string_value_segment[0]), "Hello");

  double_value_segment.append(3.14);
  EXPECT_EQ(double_value_segment.size(), 1u);
  EXPECT_EQ(type_cast<double>(double_value_segment[0]), 3.14);
}

TEST_F(StorageValueSegmentTest, AddValueOfDifferentType) {
  int_value_segment.append(3.14);
  EXPECT_EQ(int_value_segment.size(), 1u);
  EXPECT_THROW(int_value_segment.append("Hi"), std::exception);

  string_value_segment.append(3);
  string_value_segment.append(4.44);
  EXPECT_EQ(string_value_segment.size(), 2u);

  double_value_segment.append(4);
  EXPECT_EQ(double_value_segment.size(), 1u);
  EXPECT_THROW(double_value_segment.append("Hi"), std::exception);
}

TEST_F(StorageValueSegmentTest, GetAllValues) {
  int_value_segment.append(1);
  int_value_segment.append(2);
  int_value_segment.append(3);

  std::vector<int> expected_values{1, 2, 3};

  EXPECT_EQ(int_value_segment.values(), expected_values);
}

// TEST_F(StorageValueSegmentTest, MemoryUsage) {
//   int_value_segment.append(1);
//   EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{4});
//   int_value_segment.append(2);
//   EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{8});
// }

}  // namespace opossum
