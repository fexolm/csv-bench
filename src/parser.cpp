#include "parser.h"

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