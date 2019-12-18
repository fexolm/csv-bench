cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_FLAGS -msse4.2)

include_directories(lib/)

add_executable (main src/main.cpp)
