set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -O3")

aux_source_directory(common JRAFT_SRCS)
aux_source_directory(test_util JRAFT_SRCS)
aux_source_directory(tracker JRAFT_SRCS)
aux_source_directory(./ JRAFT_SRCS)

include(FindProtobuf)
file(GLOB PROTO_LIST ../pb/*.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HEADERS ${PROTO_LIST})

if (${JRAFT_SHARED_LIBRARY})
  add_library(jraft SHARED ${JRAFT_SRCS} ${PROTO_SRCS})
else()
  add_library(jraft STATIC ${JRAFT_SRCS} ${PROTO_SRCS})
endif()
