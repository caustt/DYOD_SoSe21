#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return _container.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  _container.emplace_back(type_cast<T>(val));
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return _container.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _container;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return size() * sizeof(T);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
