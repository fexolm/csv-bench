#include <cassert>
#include <chrono>
#include <csvmonkey/csvmonkey.hpp>
#include <iostream>
#include <memory>
#include <vector>

template <typename TimeT = std::chrono::milliseconds> struct measure {
  template <typename F, typename... Args>
  static typename TimeT::rep execution(F func, Args &&... args) {
    auto start = std::chrono::steady_clock::now();
    func(std::forward<Args>(args)...);
    auto duration = std::chrono::duration_cast<TimeT>(
        std::chrono::steady_clock::now() - start);
    return duration.count();
  }
};

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

void parse_chunk(std::vector<std::shared_ptr<Array>> &chunk, char *data, size_t size, char *unbuf) {
  bool finished = false;
  
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

  auto sec = measure<std::chrono::seconds>::execution([&]() {
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
          case Type::Int: {
            long val = -921384838234;
            if (cell.size != 0) {
              val = cell.as_double();
            }
            std::static_pointer_cast<TypedArray<long>>(col)->values.push_back(
                val);
          } break;

          case Type::Double: {
            double val = -921384838234;
            if (cell.size != 0) {
              val = cell.as_double();
            }
            std::static_pointer_cast<TypedArray<double>>(col)->values.push_back(
                val);
          } break;

          case Type::String: {
            std::string val = "";
            if (cell.size != 0) {
              val = cell.as_str();
            }
            std::static_pointer_cast<TypedArray<std::string>>(col)
                ->values.push_back(val);
          } break;
          }
        }
      }
      finished = row != chunk_size;
    }
  });

  std::cout << sec << " sec" << std::endl;
}