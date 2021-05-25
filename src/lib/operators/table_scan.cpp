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
  // we assume there is only one input table
  std::shared_ptr<const Table> input_table = _left_input_table();

  // filter and collect matched RowIDs
  auto matched_row_ids = _collect_matched_rows(input_table);

  // retrieve table which actually holds data
  std::shared_ptr<const Table> real_input_table = _determine_real_table(input_table);

  // create new table with reference segments containing the filtered rows
  auto output_table = _create_table_for_matched_rows(matched_row_ids, real_input_table);

  return output_table;
};

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

template <typename T>
bool TableScan::_evaluate_scan_predicate(const T& search_value, const T& value) const {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      return value == search_value;
    }
    case ScanType::OpNotEquals: {
      return value != search_value;
    }
    case ScanType::OpLessThan: {
      return value < search_value;
    }
    case ScanType::OpLessThanEquals: {
      return value <= search_value;
    }
    case ScanType::OpGreaterThan: {
      return value > search_value;
    }
    case ScanType::OpGreaterThanEquals: {
      return value >= search_value;
    }
    default: {
      throw std::logic_error("Scan type not supported");
    }
  }
}

bool TableScan::_evaluate_scan_predicate_for_dictionary_segment(const ValueID value_id, const ValueID lower_bound,
                                                     bool value_exists) const {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      if (!value_exists) {
        return false;
      } else {
        return value_id == lower_bound;
      }
    }
    case ScanType::OpNotEquals: {
      if (!value_exists) {
        return true;
      }
      return value_id != lower_bound;
    }
    case ScanType::OpLessThan: {
      return value_id < lower_bound;
    }
    case ScanType::OpLessThanEquals: {
      if (value_exists) {
        return value_id <= lower_bound;
      } else {
        return value_id < lower_bound;
      }
    }
    case ScanType::OpGreaterThan: {
      if (value_exists) {
        return value_id > lower_bound;
      } else {
        return value_id >= lower_bound;
      }
    }
    case ScanType::OpGreaterThanEquals: {
      return value_id >= lower_bound;
    }
    default: {
      throw std::logic_error("Scan type not supported");
    }
  }
}

template <typename T>
void TableScan::_collect_matched_rows_for_reference_segment(const std::shared_ptr<ReferenceSegment>& segment,
                                                 const std::shared_ptr<PosList>& matched_row_ids) const {
  for (auto const& row_id : *(segment->pos_list())) {
    const ChunkID chunk_id = row_id.chunk_id;
    const ChunkOffset real_chunk_offset = row_id.chunk_offset;

    const ColumnID column_id = segment->referenced_column_id();
    auto table = segment->referenced_table();

    const Chunk& chunk = table->get_chunk(chunk_id);
    const auto actual_segment = chunk.get_segment(column_id);

    const std::string column_type = table->column_type(_column_id);

    resolve_data_type(column_type, [&](auto type) {
      using Type = typename decltype(type)::type;
      const Type search_value = type_cast<Type>(_search_value);

      // handle value segment
      const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(actual_segment);
      if (value_segment) {
        if (_evaluate_scan_predicate<Type>(search_value, value_segment->values().at(real_chunk_offset))) {
          matched_row_ids->push_back(RowID{chunk_id, real_chunk_offset});
        }
        return;
      }

      // handle dictionary segment
      const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(actual_segment);
      if (dictionary_segment) {
        if (_evaluate_scan_predicate<Type>(search_value, dictionary_segment->get(real_chunk_offset))) {
          matched_row_ids->push_back(RowID{chunk_id, real_chunk_offset});
        }
        return;
      }
      throw std::logic_error("Unknown segment type");
    });
  }
}

template <typename T>
void TableScan::_collect_matched_rows_for_value_segment(const ChunkID chunk_id, const std::shared_ptr<ValueSegment<T>>& segment,
                                             const std::shared_ptr<PosList>& matched_row_ids) const {
  const T search_value = type_cast<T>(_search_value);
  ChunkOffset chunk_offset{0};
  for (const T& value : segment->values()) {
    if (_evaluate_scan_predicate(search_value, value)) {
      matched_row_ids->push_back(RowID{chunk_id, chunk_offset});
    }
    chunk_offset++;
  }
}

template <typename T>
void TableScan::_collect_matched_rows_for_dictionary_segment(const ChunkID chunk_id,
                                                  const std::shared_ptr<DictionarySegment<T>>& segment,
                                                  const std::shared_ptr<PosList>& matched_row_ids) const {
  const ValueID lower_bound_value = segment->lower_bound(_search_value);
  const T search_value = type_cast<T>(_search_value);

  const bool value_exists =
      (lower_bound_value != INVALID_VALUE_ID) && (segment->value_by_value_id(lower_bound_value) == search_value);

  const auto attribute_vector = segment->attribute_vector();
  const auto attribute_vector_size = attribute_vector->size();

  ValueID value_id;
  for (size_t chunk_offset = 0; chunk_offset < attribute_vector_size; ++chunk_offset) {
    value_id = attribute_vector->get(chunk_offset);
    if (_evaluate_scan_predicate_for_dictionary_segment(value_id, lower_bound_value, value_exists)) {
      matched_row_ids->push_back(RowID{chunk_id, static_cast<ChunkOffset>(chunk_offset)});
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(TableScan);

}  // namespace opossum