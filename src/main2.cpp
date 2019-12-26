#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <tbb/pipeline.h>
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
  Decimal,
  String,
};

struct Decimal {
  uint64_t a;
  uint64_t b;
  bool negative;
};

std::ostream &operator<<(std::ostream &out, const Decimal &val) {
  return out << (val.negative ? "-" : "") << val.a << "." << val.b;
}

template <typename T> constexpr Type get_runtime_type();

template <> constexpr Type get_runtime_type<long>() { return Type::Int; }

template <> constexpr Type get_runtime_type<Decimal>() { return Type::Decimal; }

template <> constexpr Type get_runtime_type<std::string>() {
  return Type::String;
}

class Array {
public:
  virtual Type type() const = 0;
  virtual void pop() = 0;
  virtual void clear() = 0;
  virtual void print(size_t i) const = 0;
  virtual size_t size() const = 0;
};

template <typename T> struct TypedArray : public Array {
  std::vector<T> values;
  constexpr Type type() const override { return get_runtime_type<T>(); }
  void pop() override { values.pop_back(); }
  void clear() override { values.clear(); }
  void print(size_t i) const override { std::cout << values[i]; }
  size_t size() const override { return values.size(); }
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

long read_int(const std::vector<char> &buf, size_t &cur, size_t size) {
  long res = 0;
  if (buf[cur] == '-') {
    res = -res;
    cur++;
  }
  while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
    res *= 10;
    res += buf[cur] - '0';
    ++cur;
  }
  ++cur;
  return res;
}

Decimal read_decimal(const std::vector<char> &buf, size_t &cur, size_t size) {
  Decimal res = {0, 0, false};
  if (buf[cur] == '-') {
    res.negative = true;
    cur++;
  }
  while (buf[cur] != ',' && buf[cur] != '\n' && buf[cur] != '.' && cur < size) {
    res.a *= 10;
    res.a += buf[cur] - '0';
    ++cur;
  }
  if (buf[cur] == '.') {
    ++cur;
    while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
      res.b *= 10;
      res.b += buf[cur] - '0';
      ++cur;
    }
  }
  ++cur;
  return res;
}

std::string read_string(const std::vector<char> &buf, size_t &cur,
                        size_t size) {
  std::string res = "";
  while (buf[cur] != ',' && buf[cur] != '\n' && cur < size) {
    res += buf[cur];
    ++cur;
  }
  ++cur;
  return res;
}

void parse_chunk(std::vector<std::shared_ptr<Array>> &chunk,
                 std::vector<char> data, size_t size) {
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

      case Type::Decimal:
        std::static_pointer_cast<TypedArray<Decimal>>(chunk[col])
            ->values.push_back(read_decimal(data, cur, size));
        break;

      case Type::String:
        std::static_pointer_cast<TypedArray<std::string>>(chunk[col])
            ->values.push_back(read_string(data, cur, size));
        break;
      }
    }
  }
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

int main() {
  // const char path[] = "/localdisk/artemale/git/omniscidb/Tests/Import/"
                      // "datafiles/trips_with_headers_top1000.csv";
  const char path[] = "/localdisk/artemale/scripts/trips_xaa.csv";
  const long buf_size = 100000;

  auto schema = {
      Type::Int,     Type::String,  Type::String,  Type::String,  Type::String,
      Type::Int,     Type::Decimal, Type::Decimal, Type::Decimal, Type::Decimal,
      Type::Int,     Type::Decimal, Type::Decimal, Type::Decimal, Type::Decimal,
      Type::Decimal, Type::Decimal, Type::Decimal, Type::Decimal, Type::Decimal,
      Type::String,  Type::Int,     Type::String,  Type::String,  Type::String,
      Type::Decimal, Type::Int,     Type::Decimal, Type::Int,     Type::Int,
      Type::Decimal, Type::Int,     Type::Decimal, Type::Int,     Type::String,
      Type::Int,     Type::Int,     Type::String,  Type::String,  Type::String,
      Type::Int,     Type::Int,     Type::Decimal, Type::Int,     Type::String,
      Type::Int,     Type::Int,     Type::String,  Type::String,  Type::String,
      Type::Int};

  FILE *f = fopen(path, "rb");
  Table table = make_table(schema);
  auto sec = measure<std::chrono::seconds>::execution([&]() {
    tbb::parallel_pipeline(
        112,
        tbb::make_filter<void, std::vector<char>>(
            tbb::filter::serial_in_order,
            [&](tbb::flow_control &fc) {
              std::vector<char> buf(buf_size);
              if (!fread(buf.data(), 1, buf_size, f)) {
                fc.stop();
                return std::vector<char>();
              }
              return buf;
            }) &
            tbb::make_filter<std::vector<char>,
                             std::vector<std::shared_ptr<Array>>>(
                tbb::filter::parallel,
                [&](std::vector<char> buf) {
                  std::vector<std::shared_ptr<Array>> columns(
                      table.columns.size());

                  for (int column_index = 0;
                       column_index < table.columns.size(); column_index++) {
                    switch (table.columns[column_index].type()) {
                    case Type::Int:
                      columns[column_index] =
                          std::make_shared<TypedArray<long>>();
                      break;

                    case Type::Decimal:
                      columns[column_index] =
                          std::make_shared<TypedArray<Decimal>>();
                      break;

                    case Type::String:
                      columns[column_index] =
                          std::make_shared<TypedArray<std::string>>();
                      break;
                    }
                  }
                  parse_chunk(columns, std::move(buf), buf_size);
                  return columns;
                }) &
            tbb::make_filter<std::vector<std::shared_ptr<Array>>, void>(
                tbb::filter::serial,
                [&](std::vector<std::shared_ptr<Array>> columns) {
                  for (int column_index = 0;
                       column_index < table.columns.size(); column_index++) {
                    table.columns[column_index].values.push_back(
                        columns[column_index]);
                  }
                }));
  });

  // print_table(table);

  std::cout << sec << " sec" << std::endl;
}