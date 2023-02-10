set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -g -fprofile-arcs \
                      -ftest-coverage -Wall -std=c++11")

add_executable(conf_change_test conf_change_test.cc)
target_link_libraries(conf_change_test gtest gtest_main gmock gmock_main
                      -Wl,--no-whole-archive ${COMMLIBS})
