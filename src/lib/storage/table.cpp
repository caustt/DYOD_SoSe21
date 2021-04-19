#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) {
  // Implementation goes here
  _target_chunk_size = target_chunk_size;
  _chunks.emplace_back(Chunk());
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  // Implementation goes here
  DebugAssert(_chunks.size() == 1, "Invalid amount of chunks");
  DebugAssert(_chunks.at(0).size() == 0, "Too many rows");

  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
    _chunks.at(0).add_segment(value_segment);
  });

  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  // Implementation goes here
  DebugAssert(_chunks.size() > 0, "No chunks present");

  if (_chunks.back().size() >= _target_chunk_size) {
    _chunks.emplace_back(Chunk());

    for (size_t column_id = 0; column_id < _column_names.size(); ++column_id) {
      auto type = _column_types.at(column_id);

      resolve_data_type(type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
        _chunks.back().add_segment(value_segment);
      });
    }
  }

  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  // Implementation goes here
}

ColumnCount Table::column_count() const {
  // Implementation goes here
  DebugAssert(_chunks.size() >= 1, "Invalid amount of chunks");

  return _chunks.at(0).column_count();
}

uint64_t Table::row_count() const {
  // Implementation goes here
  uint64_t total_row_count = 0;

  for (const auto& chunk : _chunks) {
    total_row_count += chunk.size();
  }

  return total_row_count;
}

ChunkID Table::chunk_count() const {
  // Implementation goes here
  return ChunkID{(uint16_t)_chunks.size()};
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto result = std::find(_column_names.begin(), _column_names.end(), column_name);

  if (result == _column_names.end()) {
    DebugAssert(false, "invalid column name");
  } else {
    // return ColumnID{(uint32_t) result - _column_names.begin()};
    return ColumnID{(uint16_t)std::distance(_column_names.begin(), result)};
  }
}

ChunkOffset Table::target_chunk_size() const {
  // Implementation goes here
  return _target_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  // throw std::runtime_error("Implement Table::column_names()");
  return _column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  // throw std::runtime_error("Implement Table::column_name");
  return _column_names.at(column_id);
}

const std::string& Table::column_type(const ColumnID column_id) const {
  // throw std::runtime_error("Implement Table::column_type");
  return _column_types.at(column_id);
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  //  throw std::runtime_error("Implement Table::get_chunk");
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  //  throw std::runtime_error("Implement Table::get_chunk");
  return _chunks.at(chunk_id);
}

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("Implement Table::compress_chunk"); }

}  // namespace opossum
