cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)
link_directories(${CONDA_PREFIX}/lib)
find_package(TBB REQUIRED tbb tbbmalloc tbbmalloc_proxy)

include_directories(lib/ ${TBB_INCLUDE_DIR})

add_executable (main src/main.cpp src/table.cpp src/parser.cpp)

target_link_libraries(main TBB::tbb TBB::tbbmalloc_proxy)
