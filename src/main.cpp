#include "parser.h"
#include "table.h"
#include "utils.h"
#include <tbb/pipeline.h>
#include <vector>

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
        112, tbb::make_filter<void, std::vector<char>>(
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
                       auto columns = create_empty_columns(table);
                       parse_chunk(columns, std::move(buf), buf_size);
                       return columns;
                     }) &
                 tbb::make_filter<std::vector<std::shared_ptr<Array>>, void>(
                     tbb::filter::serial,
                     [&](std::vector<std::shared_ptr<Array>> columns) {
                       for (int column_index = 0;
                            column_index < table.columns.size();
                            column_index++) {
                         table.columns[column_index].values.push_back(
                             columns[column_index]);
                       }
                     }));
  });

  // print_table(table);

  std::cout << sec << " sec" << std::endl;
}