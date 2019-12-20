cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)

include_directories(lib/)

add_executable (main src/main.cpp)
add_executable (main2 src/main2.cpp)

