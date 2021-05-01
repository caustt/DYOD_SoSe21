#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _segments.emplace_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(),
              "Invalid size " + std::to_string(values.size()) + " for chunk with size " + std::to_string(_segments.size()));
  for (size_t column_id{0}; column_id < _segments.size(); ++column_id) {
    _segments.at(column_id)->append(values.at(column_id));
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  DebugAssert(column_id < _segments.size(), "Invalid column id");
  return _segments.at(column_id);
}

ColumnCount Chunk::column_count() const { return (ColumnCount) _segments.size(); }

ChunkOffset Chunk::size() const {
  if (!_segments.empty()) {
  return _segments[0]->size();
  } else {
    return 0;
  }
}

}  // namespace opossum
