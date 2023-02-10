find_library(GTEST_LIB NAMES gtest)
if (NOT GTEST_LIB)
  message(FATAL_ERROR "Fail to find gtest")
endif()
find_library(GTEST_MAIN_LIB NAMES gtest_main)
if (NOT GTEST_MAIN_LIB)
  message(FATAL_ERROR "Fail to find gtest main")
endif()
find_library(GMOCK_LIB NAMES gmock)
if (NOT GMOCK_LIB)
  message(FATAL_ERROR "Fail to find gmock")
endif()
find_library(GMOCK_MAIN_LIB NAMES gmock_main)
if (NOT GMOCK_MAIN_LIB)
  message(FATAL_ERROR "Fail to find gmock main")
endif()

link_directories(${RPC_LIB_PATH})

set(COMMLIBS
    pthread
    jraft
    jrpc.a
    gtest
    protobuf.a)

add_subdirectory(conf_change_test)
add_subdirectory(election_test)
add_subdirectory(inflights_test)
add_subdirectory(log_unstable_test)
add_subdirectory(memory_storage_test)
add_subdirectory(progress_test)
add_subdirectory(raft_flow_control_test)
add_subdirectory(raft_log_test)
add_subdirectory(rawnode_test)
add_subdirectory(replication_test)
add_subdirectory(restore_test)
add_subdirectory(snapshot_test)
