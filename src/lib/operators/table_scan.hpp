#pragma once

#include <memory>
#include <optional>
#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>
#include <string>
#include <vector>

#include "../storage/dictionary_segment.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;

  std::shared_ptr<const Table> _on_execute() override;

 private:
  template <typename T>
  bool _evaluate_scan_predicate(const T& search_value, const T& value) const {
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

  bool _evaluate_scan_predicate_for_dictionary_segment(const ValueID value_id, const ValueID lower_bound,
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
  void _collect_matched_rows_for_reference_segment(const std::shared_ptr<ReferenceSegment>& segment,
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
  void _collect_matched_rows_for_value_segment(const ChunkID chunk_id, const std::shared_ptr<ValueSegment<T>>& segment,
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
  void _collect_matched_rows_for_dictionary_segment(const ChunkID chunk_id,
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
};

}  // namespace opossum
