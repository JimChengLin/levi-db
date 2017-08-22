cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)

add_compile_options(-msse4.2)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_compile_options(-DOS_MACOSX)
endif ()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "on")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(SOURCE_FILES main.cpp
        src/compress.h src/compress.cpp
        src/crc32c.cpp src/crc32c.h
        src/db.cpp src/db.h
        src/db_single.cpp src/db_single.h
        src/env_io.cpp src/env_io.h
        src/exception.cpp src/exception.h
        src/index.cpp src/index.h src/index_.cpp
        src/index_iter_regex.cpp src/index_iter_regex.h
        src/index_mvcc_rd.cpp src/index_mvcc_rd.h
        src/iterator.h
        src/log_reader.cpp src/log_reader.h
        src/log_writer.cpp src/log_writer.h
        src/meta_keeper.cpp src/meta_keeper.h
        src/seq_gen.cpp src/seq_gen.h
        src/slice.h
        src/util.h
        src/varint.cpp src/varint.h
        test/index_test.cpp
        test/index_mvcc_test.cpp
        test/compress_test.cpp)

add_executable(LeviDB ${SOURCE_FILES})

find_package(ZLIB)
if (ZLIB_FOUND)
    include_directories(${ZLIB_INCLUDE_DIRS})
    target_link_libraries(LeviDB ${ZLIB_LIBRARIES})
endif ()