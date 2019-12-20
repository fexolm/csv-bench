#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
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
  virtual void pop() = 0;
  virtual void clear() = 0;
};

template <typename T> struct TypedArray : public Array {
  std::vector<T> values;
  constexpr Type type() const override { return get_runtime_type<T>(); }
  void pop() override { values.pop_back(); }
  void clear() override { values.clear(); }
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

void rollback(std::vector<std::shared_ptr<Array>> &chunk, int size) {
  for (int i = 0; i < size; i++) {
    chunk[i]->pop();
  }
}

long read_int(char *buf, size_t &cur, size_t size) {
  long res = 0;
  while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
    res += buf[cur] - '0';
    ++cur;
  }
  ++cur;
  return res;
}

double read_double(char *buf, size_t &cur, size_t size) {
  double res = 0;
  while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
    res += buf[cur] - '0';
    ++cur;
  }
  ++cur;
  return res;
}

std::string read_string(char *buf, size_t &cur, size_t size) {
  std::string res = "";
  while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
    res += buf[cur];
    ++cur;
  }
  ++cur;
  return res;
}

void parse_chunk(std::vector<std::shared_ptr<Array>> &chunk, char *data,
                 size_t size) {
  size_t cur = 0;
  while (data[cur] != '\n' && cur < size)
    ++cur;
  ++cur;
  bool finished = false;
  while (!finished && cur < size) {
    for (int col = 0; col < chunk.size(); ++col) {
      if (cur == size) {
        rollback(chunk, col);
        finished = true;
        break;
      }

      switch (chunk[col]->type()) {
      case Type::Int:
        std::static_pointer_cast<TypedArray<long>>(chunk[col])
            ->values.push_back(read_int(data, cur, size));
        break;

      case Type::Double:
        std::static_pointer_cast<TypedArray<double>>(chunk[col])
            ->values.push_back(read_double(data, cur, size));
        break;

      case Type::String:
        std::static_pointer_cast<TypedArray<std::string>>(chunk[col])
            ->values.push_back(read_string(data, cur, size));
        break;
      }
    }
  }
}

int main() {
  const char path[] = "/localdisk/artemale/scripts/trips_xaa.csv";
  const long buf_size = 100000;

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

  FILE *f = fopen(path, "rb");
  char *buf = new char[buf_size];

  Table table = make_table(schema);
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
  auto sec = measure<std::chrono::seconds>::execution([&]() {
    long read = 0;
    while (fread(buf, 1, buf_size, f)) {
      parse_chunk(columns, buf, buf_size);
      for (int column_index = 0; column_index < table.columns.size();
           column_index++) {
        table.columns[column_index].values.push_back(columns[column_index]);
        columns[column_index]->clear();
      }
    }
  });

  std::cout << sec << " sec" << std::endl;
}