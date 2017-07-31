cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)

add_compile_options("-msse4.2")

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_compile_options(-DOS_MACOSX)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "on")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(SOURCE_FILES main.cpp
        src/index.cpp src/index.h src/index_misc.cpp
        src/slice.h
        src/util.h
        src/env_io.cpp src/env_io.h
        src/exception.cpp src/exception.h
        src/crc32c.cpp src/crc32c.h
        test/index_test.cpp
        src/index_mvcc.cpp src/index_mvcc.h
        src/seq_gen.cpp src/seq_gen.h)

add_executable(LeviDB ${SOURCE_FILES})