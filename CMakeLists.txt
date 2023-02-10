cmake_minimum_required(VERSION 3.16)
project(jraft VERSION 1.0)

set(CMAKE_CXX_STANDARD 11)

link_directories(third-party/jrpc/lib)

include_directories(build/src)
include_directories(include)
include_directories(third-party/jrpc/include)
include_directories(third-party/jrpc/include/jrpc)
include_directories(src)

add_subdirectory(src)
add_subdirectory(test)
