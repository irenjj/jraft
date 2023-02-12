include(FindProtobuf)
file(GLOB PROTO_LIST ${CMAKE_SOURCE_DIR}/pb/*.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HEADERS ${PROTO_LIST})

aux_source_directory(common JRAFT_SRCS)
aux_source_directory(test_util JRAFT_SRCS)
aux_source_directory(tracker JRAFT_SRCS)
aux_source_directory(./ JRAFT_SRCS)

set(JRAFT_COMMON_HEADERS
    common/conf.h
    common/noncopyable.h
    common/util.h)

set(JRAFT_TEST_UTIL_HEADERS
    test_util/network.h
    test_util/state_machine.h
    test_util/test_util.h)

set(JRAFT_TRACKER_HEADERS
    tracker/inflights.h
    tracker/progress.h
    tracker/tracker.h)

SET(JRAFT_HEADERS
    log_unstable.h
    memory_storage.h
    raft.h
    raft_log.h
    rawnode.h
    read_only.h
    ready.h
    soft_state.h
    storage.h)

add_library(jraft STATIC ${JRAFT_SRCS} ${PROTO_SRCS})


install(FILES ${JRAFT_COMMON_HEADERS} DESTINATION ${CMAKE_SOURCE_DIR}/output/include/jraft/common/)
install(FILES ${JRAFT_TEST_UTIL_HEADERS} DESTINATION ${CMAKE_SOURCE_DIR}/output/include/jraft/test_util/)
install(FILES ${JRAFT_TRACKER_HEADERS} DESTINATION ${CMAKE_SOURCE_DIR}/output/include/jraft/tracker/)
install(FILES ${JRAFT_HEADERS} DESTINATION ${CMAKE_SOURCE_DIR}/output/include/jraft/)

install(
    TARGETS jraft
    RUNTIME DESTINATION ${CMAKE_SOURCE_DIR}/output/bin/
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/output/lib/
    ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}/output/lib/
)
