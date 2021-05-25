#include "reference_segment.hpp"
#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList>& pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const RowID row_id = _pos->at(chunk_offset);
  const ChunkID chunk_id = row_id.chunk_id;
  const ChunkOffset real_chunk_offset = row_id.chunk_offset;

  const Chunk& chunk = _referenced_table->get_chunk(chunk_id);
  const auto segment = chunk.get_segment(_referenced_column_id);
  return segment->operator[](real_chunk_offset);
}

ChunkOffset ReferenceSegment::size() const { return _pos->size(); }

const std::shared_ptr<const PosList>& ReferenceSegment::pos_list() const { return _pos; }

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceSegment::estimate_memory_usage() const {
  // we assume only the size of the ReferenceSegment is needed, not the underlying segments containing the actual data
  return size() * sizeof(RowID);
}

}  // namespace opossum
