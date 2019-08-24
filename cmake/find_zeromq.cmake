if (NOT ARCH_ARM AND NOT ARCH_32 AND NOT OS_FREEBSD)
    option (ENABLE_ZEROMQ "Enable ZeroMQ" ON)
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cppzmq/CMakeLists.txt")
   message (WARNING "submodule contrib/cppzmq is missing. to fix try run: \n git submodule update --init --recursive")
   set (ENABLE_ZEROMQ 0)
endif ()

if (ENABLE_ZEROMQ)

if (OS_LINUX AND NOT ARCH_ARM AND USE_LIBGSASL)
    option (USE_INTERNAL_ZEROMQ_LIBRARY "Set to FALSE to use system zeromq instead of the bundled" ${NOT_UNBUNDLED})
endif ()

if (USE_INTERNAL_ZEROMQ_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libzmq/CMakeLists.txt")
   message (WARNING "submodule contrib/libzmq is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_ZEROMQ_LIBRARY 0)
   set (MISSING_INTERNAL_ZEROMQ_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_ZEROMQ_LIBRARY)
    find_library (ZEROMQ_LIB zmq)
    find_path (ZEROMQ_INCLUDE_DIR NAMES zmq.h zmq_utils.h PATHS ${ZEROMQ_INCLUDE_PATHS})
    set (CPPZMQ_LIBRARY cppzmq)
endif ()

if (ZEROMQ_LIB AND ZEROMQ_INCLUDE_DIR)
    set (USE_ZEROMQ 1)
    set (ZEROMQ_LIBRARY ${ZEROMQ_LIB})
    set (CPPZMQ_LIBRARY cppzmq)
elseif (NOT MISSING_INTERNAL_ZEROMQ_LIBRARY AND NOT ARCH_ARM)
    set (USE_INTERNAL_ZEROMQ_LIBRARY 1)
    set (ZEROMQ_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libzmq/src")
    set (ZEROMQ_LIBRARY zmq)
    set (CPPZMQ_LIBRARY cppzmq)
    set (USE_ZEROMQ 1)
endif ()

endif ()

message (STATUS "Using libzmq=${USE_ZEROMQ}: ${ZEROMQ_INCLUDE_DIR} : ${ZEROMQ_LIBRARY} ${CPPZMQ_LIBRARY}")