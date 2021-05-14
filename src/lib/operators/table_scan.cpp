#include "table_scan.hpp"
#include <memory>
#include <optional>
#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  std::shared_ptr<const Table> input_table = _left_input_table();

  std::vector<ReferenceSegment> output_segments;

  // filter and collect matched rowIds
  std::vector<RowID> matched_row_ids;

  std::string column_type = input_table->column_type(_column_id);

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
    const Chunk& chunk = input_table->get_chunk(chunk_id);
    std::shared_ptr<BaseSegment> segment = chunk.get_segment(_column_id);

    // handle reference segment
    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
      _collect_matched_rows_for_reference_segment(reference_segment, matched_row_ids);
    } else {
      resolve_data_type(column_type, [&](auto type) {
        using Type = typename decltype(type)::type;

        // handle value segment
        const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
        if (value_segment) {
          _collect_matched_rows_for_value_segment<Type>(chunk_id, value_segment, matched_row_ids);
          return;
        }

        // handle dictionary segment
        const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
        if (dictionary_segment) {
          _collect_matched_rows_for_dictionary_segment<Type>(chunk_id, dictionary_segment, matched_row_ids);
          return;
        }
        throw std::logic_error("Unknown segment type");
      });
    }
  }

  // ===============  matched_row_ids should be filled at this point ===============

  // TODO: create output_segments

  for (RowID row_id : matched_row_ids) {
    std::cout << "Chunk id: " << row_id.chunk_id << " Offset: " << row_id.chunk_offset << std::endl;
  }

  // 1. get result of in
  // 2. filter
  // 3. create an empty table
  // 4. fill with reference segments
  // 5. return
  return nullptr;
};

}  // namespace opossum