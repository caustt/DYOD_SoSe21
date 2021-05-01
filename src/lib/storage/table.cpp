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
  _target_chunk_size = target_chunk_size;
  _chunks.emplace_back(std::make_shared<Chunk>());
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(_chunks.size() == 1, "Invalid amount of chunks");
  DebugAssert(row_count() == 0, "Too many rows");

  _add_segment(type);

  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
}

void Table::_add_segment(const std::string& type) {
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
    _chunks.back()->add_segment(value_segment);
  });
}

void Table::_create_new_chunk() {
  _chunks.emplace_back(std::make_shared<Chunk>());
  for (size_t column_id = 0; column_id < _column_names.size(); ++column_id) {
    auto type = _column_types.at(column_id);
    _add_segment(type);
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(_chunks.size() > 0, "No chunks present");

  if (_chunks.back()->size() >= _target_chunk_size) {
    _create_new_chunk();
  }

  _chunks.back()->append(values);
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
  uint64_t total_row_count = 0;

  for (const auto& chunk : _chunks) {
    total_row_count += chunk->size();
  }

  return total_row_count;
}

ChunkID Table::chunk_count() const { return (ChunkID) _chunks.size(); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto result = std::find(_column_names.begin(), _column_names.end(), column_name);

  if (result == _column_names.end()) {
    DebugAssert(false, "invalid column name");
  } else {
    return ColumnID{(uint16_t)std::distance(_column_names.begin(), result)};
  }
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) {
  const Chunk& chunk = get_chunk(chunk_id);
  std::shared_ptr<Chunk> encoded_chunk = Chunk::apply_dictionary_encoding(chunk, _column_types);
  _chunks[chunk_id] = encoded_chunk;  //TODO: chweck if suitabl
}

}  // namespace opossum
