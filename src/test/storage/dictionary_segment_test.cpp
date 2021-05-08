#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/storage/dictionary_segment.hpp"

namespace opossum {

template <typename T>
class DictionarySegmentFixture : public DictionarySegment<T> {
 public:
  DictionarySegmentFixture(std::shared_ptr<opossum::ValueSegment<T>> value_segment)
      : DictionarySegment<T>(value_segment) {}
  int get_minimal_number_of_bits_for_dictionary_size(size_t size) {
    return this->_get_minimal_number_of_bits_for_dictionary_size(size);
  }
  std::shared_ptr<BaseAttributeVector> create_attribute_vector_with_type(int minimal_bytes,
                                                                         std::shared_ptr<ValueSegment<T>> value_segment,
                                                                         std::shared_ptr<std::vector<T>> dictionary) {
    return this->_create_attribute_vector_with_type(minimal_bytes, value_segment, dictionary);
  };
};

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto dict_col = std::make_shared<DictionarySegment<std::string>>(vc_str);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  auto dict_col = std::make_shared<DictionarySegment<int>>(vc_int);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, MemoryUsage) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  vc_int->append(10);
  vc_int->append(10);

  auto dict_col = std::make_shared<DictionarySegment<int>>(vc_int);

  // values:                  0, 2, 4, 6, 8, 10, 10, 10
  // dictionary size:         6 * sizeof(int)
  // attribute vector size:   8 * sizeof(uint8_t)
  EXPECT_EQ(dict_col->estimate_memory_usage(), 6 * sizeof(int) + 8 * sizeof(uint8_t));
}

TEST_F(StorageDictionarySegmentTest, BracketOperator) {
  vc_str->append("World");
  vc_str->append("Hallo");
  vc_str->append("World");

  auto dict_col = std::make_shared<DictionarySegment<std::string>>(vc_str);

  EXPECT_EQ(dict_col->operator[](1), AllTypeVariant{"Hallo"});
}

TEST_F(StorageDictionarySegmentTest, AppendNotPossible) {
  auto dict_col = std::make_shared<DictionarySegment<std::string>>(vc_str);

  EXPECT_THROW(dict_col->append(AllTypeVariant{"Hallo"}), std::runtime_error);
}

TEST_F(StorageDictionarySegmentTest, CorrectNumberOfBits) {
  auto dictionary_segment_fixture = std::make_shared<DictionarySegmentFixture<int>>(vc_int);

  int minimal_number8 = dictionary_segment_fixture->get_minimal_number_of_bits_for_dictionary_size(
      std::numeric_limits<uint8_t>::max() - 1);
  int minimal_number16 = dictionary_segment_fixture->get_minimal_number_of_bits_for_dictionary_size(
      std::numeric_limits<uint16_t>::max() - 1);
  int minimal_number32 = dictionary_segment_fixture->get_minimal_number_of_bits_for_dictionary_size(
      std::numeric_limits<uint32_t>::max() - 1);
  int minimal_number64 = dictionary_segment_fixture->get_minimal_number_of_bits_for_dictionary_size(
      std::numeric_limits<uint64_t>::max() - 1);

  EXPECT_EQ(minimal_number8, 8);
  EXPECT_EQ(minimal_number16, 16);
  EXPECT_EQ(minimal_number32, 32);
  EXPECT_EQ(minimal_number64, 64);
}

TEST_F(StorageDictionarySegmentTest, CreateAttributeVector) {
  auto dictionary_segment_fixture = std::make_shared<DictionarySegmentFixture<int>>(vc_int);
  std::vector<int> dictionary_vector{1, 2, 3};
  auto dictionary = std::make_shared<std::vector<int>>(dictionary_vector);

  vc_int->append(2);
  vc_int->append(3);
  vc_int->append(1);
  vc_int->append(1);

  int minimal_bytes = 16;
  auto attribute_vector =
      dictionary_segment_fixture->create_attribute_vector_with_type(minimal_bytes, vc_int, dictionary);

  EXPECT_EQ(attribute_vector->width(), 2);
  EXPECT_EQ(attribute_vector->size(), 4);
  EXPECT_EQ(attribute_vector->get(0), 1);
  EXPECT_EQ(attribute_vector->get(1), 2);
  EXPECT_EQ(attribute_vector->get(2), 0);
  EXPECT_EQ(attribute_vector->get(3), 0);

  minimal_bytes = 32;
  attribute_vector = dictionary_segment_fixture->create_attribute_vector_with_type(minimal_bytes, vc_int, dictionary);
  EXPECT_EQ(attribute_vector->width(), 4);

  minimal_bytes = 64;
  attribute_vector = dictionary_segment_fixture->create_attribute_vector_with_type(minimal_bytes, vc_int, dictionary);
  EXPECT_EQ(attribute_vector->width(), 8);
}

}  // namespace opossum
