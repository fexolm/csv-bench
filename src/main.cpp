#include <cassert>
#include <csvmonkey/csvmonkey.hpp>
#include <iostream>
#include <memory>
#include <vector>
enum class Type {
  Int,
  Double,
  String,
};

template <typename T> constexpr Type get_runtime_type();

template <> constexpr Type get_runtime_type<long>() { return Type::Int; }

template <> constexpr Type get_runtime_type<double>() { return Type::Double; }

template <> constexpr Type get_runtime_type<std::string>() {
  return Type::String;
}

class Array {
public:
  virtual Type type() const = 0;
};

template <typename T> struct TypedArray : public Array {
  std::vector<T> values;
  constexpr Type type() const override { return get_runtime_type<T>(); }
};

class ChunkedArray {
private:
  Type m_type;

public:
  std::vector<std::shared_ptr<Array>> values;

  explicit ChunkedArray(Type type) : m_type(type) {}

  Type type() const { return m_type; }
};

struct Table {
  std::vector<ChunkedArray> columns;
};

Table make_table(std::vector<Type> schema) {
  Table res;
  for (auto type : schema) {
    res.columns.push_back(ChunkedArray(type));
  }
  return res;
}

int main() {
  const char path[] = "/localdisk/artemale/scripts/trips_xaa.csv";
  const long chunk_size = 100000;

  csvmonkey::MappedFileCursor stream;
  csvmonkey::CsvReader<csvmonkey::MappedFileCursor> reader(stream);
  stream.open(path);

  auto schema = {
      Type::Int,    Type::String, Type::String, Type::String, Type::String,
      Type::Int,    Type::Double, Type::Double, Type::Double, Type::Double,
      Type::Int,    Type::Double, Type::Double, Type::Double, Type::Double,
      Type::Double, Type::Double, Type::Double, Type::Double, Type::Double,
      Type::String, Type::Int,    Type::String, Type::String, Type::String,
      Type::Double, Type::Int,    Type::Double, Type::Int,    Type::Int,
      Type::Double, Type::Int,    Type::Double, Type::Int,    Type::String,
      Type::Int,    Type::Int,    Type::String, Type::String, Type::String,
      Type::Int,    Type::Int,    Type::Double, Type::Int,    Type::String,
      Type::Int,    Type::Int,    Type::String, Type::String, Type::String,
      Type::Int};

  Table table = make_table(schema);

  bool finished = false;
  for (int chunk = 0; !finished; chunk++) {
    std::vector<std::shared_ptr<Array>> columns(table.columns.size());
    for (int column_index = 0; column_index < table.columns.size();
         column_index++) {
      switch (table.columns[column_index].type()) {
      case Type::Int:
        columns[column_index] = std::make_shared<TypedArray<long>>();
        break;

      case Type::Double:
        columns[column_index] = std::make_shared<TypedArray<double>>();
        break;

      case Type::String:
        columns[column_index] = std::make_shared<TypedArray<std::string>>();
        break;
      }
    }
    int row = 0;
    for (; row < chunk_size && reader.read_row(); row++) {
      auto &row = reader.row();
      for (int column_index = 0; column_index < columns.size();
           column_index++) {
        auto &cell = row.cells[column_index];
        auto &col = columns[column_index];

        switch (columns[column_index]->type()) {
        case Type::Int:
          std::static_pointer_cast<TypedArray<long>>(col)->values.push_back(cell.as_double());
          break;

        case Type::Double:
          std::static_pointer_cast<TypedArray<double>>(col)->values.push_back(cell.as_double());
          break;

        case Type::String:
          std::static_pointer_cast<TypedArray<std::string>>(col)->values.push_back(cell.as_str());
          break;
        }
      }
    }
    finished = row != chunk_size;
  }
}