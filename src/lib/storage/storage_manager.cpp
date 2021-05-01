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
  DebugAssert(!has_table(name), "Table with the given name already exists");
  _table_mapping[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  DebugAssert(has_table(name), "Table with the given name doesn't exists");
  _table_mapping.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _table_mapping.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _table_mapping.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  for (auto const& element : _table_mapping) {
    table_names.push_back(element.first);
  }
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto const& mapping : _table_mapping) {
    auto table = mapping.second;
    auto table_name = mapping.first;
    auto columns = table->column_count();
    auto rows = table->row_count();
    auto chunks = table->chunk_count();

    out << "Table name: " << table_name << ", #columns: " << columns << ", #rows: " << rows << ", #chunks: " << chunks
        << '\n';
  }
}

void StorageManager::reset() { _table_mapping.clear(); }

}  // namespace opossum
