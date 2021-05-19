#include "table_scan.hpp"
#include <memory>
#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>
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

  // filter and collect matched rowIds

  auto matched_row_ids = std::make_shared<std::vector<RowID>>();

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

  for (RowID row_id : *matched_row_ids) {
    std::cout << "Chunk id: " << row_id.chunk_id << " Offset: " << row_id.chunk_offset << std::endl;
  }

  //  Chunk reference_chunk =
  std::vector<ReferenceSegment> output_segments;

  auto output_table = std::make_shared<Table>();
  auto output_chunk = std::make_shared<Chunk>();

  ColumnCount column_count = input_table->column_count();

  std::shared_ptr<const Table> real_input_table;
  auto first_segment = input_table->get_chunk(ChunkID{0}).get_segment(ColumnID{0});
  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(first_segment);
  if (reference_segment) {
    real_input_table = reference_segment->referenced_table();
  } else {
    real_input_table = input_table;
  }

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    auto new_segment = std::make_shared<ReferenceSegment>(real_input_table, column_id, matched_row_ids);
    output_chunk->add_segment(new_segment);
    output_table->add_column(input_table->column_name(column_id), input_table->column_type(column_id));
  }

  //  output_table->emplace_chunk(std::move(output_chunk));
  output_table->emplace_chunk(output_chunk);

  return output_table;

  //  create table
  //  new chunk
  //  for column in columns
  //
  //        create reference segment
  //        add to chunk
  //  add chunk to new table

  // 1. get result of in
  // 2. filter
  // 3. create an empty table
  // 4. fill with reference segments
  // 5. return

  //return nullptr;
};

}  // namespace opossum