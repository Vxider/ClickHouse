set(CPPZMQ_DIR ${ClickHouse_SOURCE_DIR}/contrib/cppzmq)

set(SRCS
    ${CPPZMQ_DIR}/include/cppzmq/zmq.hpp
    ${CPPZMQ_DIR}/include/cppzmq/zmq_addon.hpp
)

# add_library(cppzmq INTERFACE)
add_library(cppzmq ${SRCS})
set_target_properties(cppzmq PROPERTIES LINKER_LANGUAGE CXX) 
target_link_libraries(cppzmq PRIVATE ${ZEROMQ_LIBRARY})
# MESSAGE("AAAAAAAAAAAAAAAAA_LIBUNWIND:${UNWIND_LIBRARY}")
# target_link_libraries(cppzmq PUBLIC ${UNWIND_LIBRARY})

target_include_directories(cppzmq PRIVATE ${CPPZMQ_DIR}/include/cppzmq)
target_include_directories(cppzmq SYSTEM PUBLIC ${CPPZMQ_DIR}/include)
