cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)

add_compile_options(-msse4.2 -pthread)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_compile_options(-DOS_MACOSX)
    #    add_compile_options(-DLEVI_BENCH) # profile
endif ()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "on")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(SOURCE_FILES main.cpp
        bench/db_read_bench.cpp
        bench/db_write_bench.cpp
        bench/kv_read_bench.cpp
        bench/kv_write_bench.cpp
        src/aggregator/compact.h
        src/aggregator/compact_1_2.cpp src/aggregator/compact_1_2.h
        src/aggregator/compact_2_1.cpp src/aggregator/compact_2_1.h
        src/compress.h src/compress.cpp
        src/crc32c.cpp src/crc32c.h
        src/db.cpp src/db.h
        src/db_single.cpp src/db_single.h
        src/env_io.cpp src/env_io.h
        src/env_thread.cpp src/env_thread.h
        src/exception.cpp src/exception.h
        src/index.cpp src/index.h src/index_.cpp
        src/index_iter_regex.cpp src/index_iter_regex.h
        src/index_mvcc_rd.cpp src/index_mvcc_rd.h
        src/iterator.h
        src/levi_regex/r.cpp src/levi_regex/r.h src/levi_regex/r_.cpp
        src/levi_regex/result.h
        src/levi_regex/state_machine.cpp src/levi_regex/state_machine.h
        src/log_reader.cpp src/log_reader.h
        src/log_writer.cpp src/log_writer.h
        src/merger.h
        src/meta_keeper.h
        src/optional.h
        src/options.h
        src/seq_gen.cpp src/seq_gen.h
        src/slice.h
        src/usr.cpp src/usr.h
        src/util.h
        src/varint.cpp src/varint.h
        test/compress_test.cpp
        test/db_single_test.cpp
        test/env_io_test.cpp
        test/index_iter_test.cpp
        test/index_mvcc_test.cpp
        test/index_rd_test.cpp
        test/index_regex_test.cpp
        test/index_test.cpp
        test/log_test.cpp
        test/misc_test.cpp
        test/regex_test.cpp
        src/aggregator/aggregator.cpp src/aggregator/aggregator.h)

add_executable(LeviDB ${SOURCE_FILES})

find_package(ZLIB REQUIRED)
if (ZLIB_FOUND)
    include_directories(${ZLIB_INCLUDE_DIRS})
    target_link_libraries(LeviDB ${ZLIB_LIBRARIES})
endif ()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(LeviDB Threads::Threads)