#pragma once

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(size_t size) { _attributes = std::vector<T>(size); }

  // returns the value id at a given position
  ValueID get(const size_t i) const override { return (ValueID)_attributes.at(i); };

  // sets the value id at a given position
  void set(const size_t i, const ValueID value_id) override { _attributes.at(i) = (T)value_id; }

  // returns the number of values
  size_t size() const override { return _attributes.size(); }

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const override { return sizeof(T); }

 private:
  std::vector<T> _attributes;
};

}  // namespace opossum