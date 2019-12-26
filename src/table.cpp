#include "table.h"
#include <cassert>

std::ostream &operator<<(std::ostream &out, const Decimal &val) {
  return out << (val.negative ? "-" : "") << val.a << "." << val.b;
}

Table make_table(std::vector<Type> schema) {
  Table res;
  for (auto type : schema) {
    res.columns.push_back(ChunkedArray(type));
  }
  return res;
}

void print_table(const Table &table) {
  size_t chunks_count = table.columns[0].values.size();
  for (auto &c : table.columns) {
    assert(c.values.size() == chunks_count);
  }
  for (size_t chunk = 0; chunk < chunks_count; chunk++) {
    size_t rows_count = table.columns[0].values[chunk]->size();
    for (auto &c : table.columns) {
      assert(c.values[chunk]->size() == rows_count);
    }
  }
  for (size_t chunk = 0; chunk < chunks_count; chunk++) {
    size_t rows_count = table.columns[0].values[chunk]->size();
    for (size_t row = 0; row < rows_count; row++) {
      for (auto &c : table.columns) {
        c.values[chunk]->print(row);
        std::cout << " ";
      }
      std::cout << "\n";
    }
  }
}

std::vector<std::shared_ptr<Array>> create_empty_columns(const Table &table) {
  std::vector<std::shared_ptr<Array>> columns(table.columns.size());

  for (int column_index = 0; column_index < table.columns.size();
       column_index++) {
    switch (table.columns[column_index].type()) {
    case Type::Int:
      columns[column_index] = std::make_shared<TypedArray<long>>();
      break;

    case Type::Decimal:
      columns[column_index] = std::make_shared<TypedArray<Decimal>>();
      break;

    case Type::String:
      columns[column_index] = std::make_shared<TypedArray<std::string>>();
      break;
    }
  }
  return columns;
}
