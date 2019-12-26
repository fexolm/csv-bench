#pragma once

#include <iostream>
#include <memory>
#include <vector>

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

std::ostream &operator<<(std::ostream &out, const Decimal &val);

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

  inline explicit ChunkedArray(Type type) : m_type(type) {}

  inline Type type() const { return m_type; }
};

struct Table {
  std::vector<ChunkedArray> columns;
};

Table make_table(std::vector<Type> schema);

void print_table(const Table &table);

std::vector<std::shared_ptr<Array>> create_empty_columns(const Table &table);