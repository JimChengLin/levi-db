cmake_minimum_required(VERSION 3.8)
project(levidb8)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

set(CMAKE_CXX_STANDARD 14)

add_compile_options(-msse4.2)

set(DO_BENCH OFF) # change "OFF" to "ON", if bench
if (DO_BENCH)
    add_compile_options(-DLEVI_BENCH)
endif ()

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_compile_options(-DOS_MACOSX)
endif ()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "on")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(SOURCE_FILES main.cpp
        bench/db_bench.cpp
        src/compress.cpp src/compress.h
        src/config.h
        src/crc32c.cpp src/crc32c.h
        src/db.cpp src/db.h
        src/env_io.cpp src/env_io.h
        src/env_thread.cpp src/env_thread.h
        src/exception.cpp src/exception.h
        src/index.cpp src/index.h src/index_.cpp
        src/index_internal.cpp src/index_internal.h
        src/index_read.cpp src/index_read.h
        src/index_scan.cpp
        src/iterator.h
        src/keeper.h
        src/log_reader.cpp src/log_reader.h
        src/log_writer.cpp src/log_writer.h
        src/optional.h
        src/options.h
        src/slice.h
        src/usr.cpp src/usr.h
        src/util.h
        src/varint.cpp src/varint.h
        test/compress_test.cpp
        test/db_test.cpp
        test/index_iter_test.cpp
        test/index_iter_thread_test.cpp
        test/index_read_test.cpp
        test/index_test.cpp
        test/index_thread_test.cpp
        test/log_test.cpp
        )

add_executable(levidb8 ${SOURCE_FILES})

find_package(zstd REQUIRED)
include_directories(${ZSTD_INCLUDE_DIR})
target_link_libraries(levidb8 ${ZSTD_LIBRARIES})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
    find_package(JeMalloc REQUIRED)
    include_directories(${JEMALLOC_INCLUDE_DIR})
    target_link_libraries(levidb8 ${JEMALLOC_LIBRARIES})
endif ()

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    target_link_libraries(levidb8 Threads::Threads)

    if (DO_BENCH)
        find_package(ZLIB REQUIRED)
        include_directories(${ZLIB_INCLUDE_DIR})
        target_link_libraries(levidb8 ${ZLIB_LIBRARIES})
    endif ()
endif ()

if (DO_BENCH)
    find_package(RocksDB REQUIRED)
    include_directories(${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(levidb8 ${ROCKSDB_LIBRARIES})
endif ()