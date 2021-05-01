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
#include "dictionary_segment.hpp"
#include "resolve_type.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _segments.emplace_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(),
              "Invalid size " + std::to_string(values.size()) + " for chunk with size " + std::to_string(_segments.size()));
  auto segments_size = _segments.size();
  for (size_t column_id{0}; column_id < segments_size; ++column_id) {
    _segments.at(column_id)->append(values.at(column_id));
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _segments.at(column_id); }

ColumnCount Chunk::column_count() const { return (ColumnCount) _segments.size(); }

ChunkOffset Chunk::size() const {
  if (!_segments.empty()) {
  return _segments[0]->size();
  } else {
    return 0;
  }
}

std::shared_ptr<Chunk> Chunk::apply_dictionary_encoding(const Chunk& chunk,
                                                        const std::vector<std::string>& column_names) {
  std::shared_ptr<Chunk> encoded_chunk = std::make_shared<Chunk>();
  auto column_count = chunk.column_count();
  for (ColumnCount column_id{0}; column_id < column_count; column_id++) {
    std::shared_ptr<BaseSegment> segment = chunk.get_segment(ColumnID{column_id});
    auto type = column_names.at(column_id);

    resolve_data_type(type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      std::shared_ptr<DictionarySegment<ColumnDataType>> encoded_segment =
                                                             std::make_shared<DictionarySegment<ColumnDataType>>(segment);
      encoded_chunk->add_segment(encoded_segment);
    });
  }

  return encoded_chunk;
}

}  // namespace opossum
