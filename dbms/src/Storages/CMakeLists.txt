add_subdirectory(System)
add_subdirectory(Kafka)
add_subdirectory(Socket)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
