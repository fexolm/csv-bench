#pragma once
#include "table.h"

void parse_chunk(std::vector<std::shared_ptr<Array>> &chunk,
                 std::vector<char> data, size_t size);