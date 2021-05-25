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
  std::shared_ptr<std::vector<RowID>> _collect_matched_rows(std::shared_ptr<const Table>& input_table) const;
  void _collect_matched_rows_for_segment(std::shared_ptr<BaseSegment>& segment, std::string& column_type,
                                         ChunkID chunk_id, std::shared_ptr<std::vector<RowID>>& matched_row_ids) const;

  std::shared_ptr<const Table> _determine_real_table(std::shared_ptr<const Table>& input_table) const;
  std::shared_ptr<Table> _create_table_for_matched_rows(std::shared_ptr<std::vector<RowID>>& matched_row_ids,
                                                        std::shared_ptr<const Table>& real_input_table) const;

  template <typename T>
  bool _evaluate_scan_predicate(const T& search_value, const T& value) const;

  bool _evaluate_scan_predicate_for_dictionary_segment(const ValueID value_id, const ValueID lower_bound,
                                                       bool value_exists) const;

  template <typename T>
  void _collect_matched_rows_for_reference_segment(const std::shared_ptr<ReferenceSegment>& segment,
                                                   const std::shared_ptr<PosList>& matched_row_ids) const;

  template <typename T>
  void _collect_matched_rows_for_value_segment(const ChunkID chunk_id, const std::shared_ptr<ValueSegment<T>>& segment,
                                               const std::shared_ptr<PosList>& matched_row_ids) const;

  template <typename T>
  void _collect_matched_rows_for_dictionary_segment(const ChunkID chunk_id,
                                                    const std::shared_ptr<DictionarySegment<T>>& segment,
                                                    const std::shared_ptr<PosList>& matched_row_ids) const;

};

#define EXPLICIT_INSTANTIATION(r, template_class, type) template class template_class<type>;

// Explicitly instantiates the given template class for all types in data_types
#define EXPLICITLY_INSTANTIATE_DATA_TYPES(template_class)                         \
  BOOST_PP_SEQ_FOR_EACH(EXPLICIT_INSTANTIATION, template_class, data_types_macro) \
  static_assert(true, "End call of macro with a semicolon")

}  // namespace opossum
