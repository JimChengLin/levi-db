cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    add_compile_options(-DOS_MACOSX)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "on")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(SOURCE_FILES
        main.cpp
        benchmark/compress_bench.cpp
        src/arena.cpp src/arena.h
        src/coder.cpp src/coder.h
        src/coding.cpp src/coding.h
        src/compressor.cpp src/compressor.h
        src/crc32c.cpp src/crc32c.h
        src/decompressor.cpp src/decompressor.h
        src/env_io.cpp src/env_io.h
        src/env_thread.cpp src/env_thread.h
        src/exception.cpp src/exception.h
        src/index.cpp src/index.h
        src/iterator.h
        src/log_writer.cpp src/log_writer.h
        src/matcher.cpp src/matcher.h
        src/options.h
        src/random.h
        src/repeat_detector.cpp src/repeat_detector.h
        src/skiplist.h
        src/slice.h
        src/util.h
        test/coder_test.cpp
        test/compressor_test.cpp
        test/crc32c_test.cpp
        test/index_test.cpp
        test/log_writer_test.cpp
        test/repeat_detector_test.cpp
        test/skiplist_test.cpp
        )

add_executable(LeviDB ${SOURCE_FILES})