#include "table_scan.hpp"
#include <memory>
#include <utility>
#include <vector>
#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  // we assume there is only one input table
  std::shared_ptr<const Table> input_table = _left_input_table();

  // filter and collect matched RowIDs
  auto matched_row_ids = _collect_matched_rows(input_table);

  // retrieve table which actually holds data
  std::shared_ptr<const Table> real_input_table = _determine_real_table(input_table);

  // create new table with reference segments containing the filtered rows
  auto output_table = _create_table_for_matched_rows(matched_row_ids, real_input_table);

  return output_table;
}

std::shared_ptr<const Table> TableScan::_determine_real_table(std::shared_ptr<const Table>& input_table) const {
  auto first_segment = input_table->get_chunk(ChunkID{0}).get_segment(ColumnID{0});
  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(first_segment);
  if (reference_segment) {
    return reference_segment->referenced_table();
  } else {
    return input_table;
  }
}

std::shared_ptr<Table> TableScan::_create_table_for_matched_rows(std::shared_ptr<std::vector<RowID>>& matched_row_ids,
                                                                 std::shared_ptr<const Table>& real_input_table) const {
  ColumnCount column_count = real_input_table->column_count();
  auto output_table = std::make_shared<Table>();
  auto output_chunk = Chunk();

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    auto new_segment = std::make_shared<ReferenceSegment>(real_input_table, column_id, matched_row_ids);
    output_chunk.add_segment(new_segment);
    output_table->add_column_definition(real_input_table->column_name(column_id),
                                        real_input_table->column_type(column_id));
  }

  output_table->emplace_chunk(std::move(output_chunk));

  return output_table;
}

std::shared_ptr<std::vector<RowID>> TableScan::_collect_matched_rows(std::shared_ptr<const Table>& input_table) const {
  auto matched_row_ids = std::make_shared<std::vector<RowID>>();
  std::string column_type = input_table->column_type(_column_id);

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
    const Chunk& chunk = input_table->get_chunk(chunk_id);
    std::shared_ptr<BaseSegment> segment = chunk.get_segment(_column_id);

    _collect_matched_rows_for_segment(segment, column_type, chunk_id, matched_row_ids);
  }
  return matched_row_ids;
}

void TableScan::_collect_matched_rows_for_segment(std::shared_ptr<BaseSegment>& segment, std::string& column_type,
                                                  ChunkID chunk_id,
                                                  std::shared_ptr<std::vector<RowID>>& matched_row_ids) const {
  resolve_data_type(column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    // handle reference segment
    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    if (reference_segment) {
      _collect_matched_rows_for_reference_segment<Type>(reference_segment, matched_row_ids);
      return;
    }

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

}  // namespace opossum
