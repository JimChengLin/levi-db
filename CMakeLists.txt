cmake_minimum_required(VERSION 3.8)
project(levidb8)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

set(CMAKE_CXX_STANDARD 14)

add_compile_options(-msse4.2)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_definitions(-DOS_MACOSX)
endif ()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "ON")
    list(APPEND CMAKE_CXX_FLAGS "-fprofile-arcs -ftest-coverage")
endif ()

set(DO_BENCH OFF) # change "OFF" to "ON", if bench
if (DO_BENCH)
    add_definitions(-DLEVI_BENCH)
endif ()

set(LEVI_SOURCE_FILES
        include/iterator.h
        include/options.h
        include/slice.h
        src/compress.cpp src/compress.h
        src/config.h
        src/crc32c.cpp src/crc32c.h
        src/db.cpp include/db.h
        src/env_io.cpp src/env_io.h
        src/env_thread.cpp src/env_thread.h
        src/exception.cpp include/exception.h
        src/index.h src/index.hpp src/index_scan.hpp
        src/index_internal.cpp src/index_internal.h
        src/index_read.cpp src/index_read.h
        src/keeper.h
        src/log_reader.cpp src/log_reader.h
        src/log_writer.cpp src/log_writer.h
        src/optional.h
        src/simple_iterator.h
        src/usr.cpp src/usr.h
        src/util.h
        src/varint.cpp src/varint.h
        )

set(LEVI_TEST_FILES main.cpp
        bench/db_bench.cpp
        src/index_debug.h
        test/compress_test.cpp
        test/db_test.cpp
        test/index_iter_test.cpp
        test/index_iter_thread_test.cpp
        test/index_read.cpp
        test/index_test.cpp
        test/index_thread_test.cpp
        test/log_test.cpp
        )

add_executable(levidb8 ${LEVI_SOURCE_FILES} ${LEVI_TEST_FILES})
add_library(levidb8-shared SHARED ${LEVI_SOURCE_FILES})
add_library(levidb8-static STATIC ${LEVI_SOURCE_FILES})

find_package(zstd REQUIRED)
include_directories(${ZSTD_INCLUDE_DIR})
list(APPEND THIRDPARTY_LIBS ${ZSTD_LIBRARIES})

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    list(APPEND THIRDPARTY_LIBS Threads::Threads)
endif ()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
    find_package(JeMalloc REQUIRED)
    list(APPEND THIRDPARTY_LIBS ${JEMALLOC_LIBRARIES})
endif ()

if (DO_BENCH)
    find_package(RocksDB REQUIRED)
    include_directories(${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(levidb8 ${ROCKSDB_LIBRARIES})
endif ()

target_link_libraries(levidb8 ${THIRDPARTY_LIBS})
target_link_libraries(levidb8-shared ${THIRDPARTY_LIBS})
target_link_libraries(levidb8-static ${THIRDPARTY_LIBS})