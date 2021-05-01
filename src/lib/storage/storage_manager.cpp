#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager storage_manager = StorageManager();
  return storage_manager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _table_names.emplace_back(name);
  _tables.emplace_back(table);
}

void StorageManager::drop_table(const std::string& name) {
  size_t index = _get_index_for_table(name);
  _tables.erase(_tables.begin() + index);
  _table_names.erase(_table_names.begin() + index);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  auto index = _get_index_for_table(name);
  return _tables.at(index);
}

size_t StorageManager::_get_index_for_table(const std::string& name) const {
  auto result = std::find(_table_names.begin(), _table_names.end(), name);
  if (result == _table_names.end()) {
    throw std::runtime_error("Table not found.");
  }
  return std::distance(_table_names.begin(), result);
}

bool StorageManager::has_table(const std::string& name) const {
  return std::find(_table_names.begin(), _table_names.end(), name) != _table_names.end();
}

std::vector<std::string> StorageManager::table_names() const { return _table_names; }

void StorageManager::print(std::ostream& out) const {
  for (size_t index{0}; index < _tables.size(); ++index) {
    auto table = _tables.at(index);
    auto table_name = _table_names.at(index);
    auto columns = table->column_count();
    auto rows = table->row_count();
    auto chunks = table->chunk_count();

    out << "Table name: " << table_name << ", #columns: " << columns << ", #rows: " << rows << ", #chunks: " << chunks
        << '\n';
  }
}

void StorageManager::reset() {
  _tables.clear();
  _table_names.clear();
}

}  // namespace opossum
