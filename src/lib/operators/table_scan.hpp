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
  bool _evaluate_scan_predicate(const T& value) {
    const T search_value = type_cast<T>(_search_value);
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

  void _collect_matched_rows_for_reference_segment(std::shared_ptr<ReferenceSegment> segment,
                                                   std::shared_ptr<PosList> matched_row_ids) {
    for (auto const& row_id : *(segment->pos_list())) {
      const ChunkID chunk_id = row_id.chunk_id;
      const ChunkOffset real_chunk_offset = row_id.chunk_offset;

      ColumnID column_id = segment->referenced_column_id();
      auto table = segment->referenced_table();

      const Chunk& chunk = table->get_chunk(chunk_id);
      const auto actual_segment = chunk.get_segment(column_id);

      std::string column_type = table->column_type(_column_id);

      resolve_data_type(column_type, [&](auto type) {
        using Type = typename decltype(type)::type;

        // handle value segment
        const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(actual_segment);
        if (value_segment) {
          if (_evaluate_scan_predicate(value_segment->values().at(real_chunk_offset))) {
            matched_row_ids->push_back(RowID{chunk_id, real_chunk_offset});
          }
          return;
        }

        // handle dictionary segment
        const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(actual_segment);
        if (dictionary_segment) {
          if (_evaluate_scan_predicate(dictionary_segment->get(real_chunk_offset))) {
            matched_row_ids->push_back(RowID{chunk_id, real_chunk_offset});
          }
          return;
        }
        throw std::logic_error("Unknown segment type");
      });
    }
  }

  template <typename T>
  void _collect_matched_rows_for_value_segment(ChunkID chunk_id, std::shared_ptr<ValueSegment<T>> segment,
                                               std::shared_ptr<PosList> matched_row_ids) {
    ChunkOffset chunk_offset{0};
    for (const T& value : segment->values()) {
      if (_evaluate_scan_predicate(value)) {
        matched_row_ids->push_back(RowID{chunk_id, chunk_offset});
      }
      chunk_offset++;
    }
  }

  template <typename T>
  void _collect_matched_rows_for_dictionary_segment(ChunkID chunk_id, std::shared_ptr<DictionarySegment<T>> segment,
                                                    std::shared_ptr<PosList> matched_row_ids) {
    // trivial solution: decompress every value id and then compare it to the
    // search value
    // TODO: better solution (see Sprint document)

    for (ChunkOffset chunk_offset{0}; chunk_offset < segment->size(); ++chunk_offset) {
      const T value = segment->get(chunk_offset);
      if (_evaluate_scan_predicate(value)) {
        matched_row_ids->push_back(RowID{chunk_id, chunk_offset});
      }
    }
  }
};

}  // namespace opossum
