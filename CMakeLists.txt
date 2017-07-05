cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)
add_compile_options(-DOS_MACOSX) # 在 Mac 上开发

set(SOURCE_FILES
        main.cpp
        src/arena.cpp src/arena.h
        src/coder.cpp src/coder.h
        src/compressor.cpp src/compressor.h
        src/exception.cpp src/exception.h
        src/env_io.cpp src/env_thread.cpp src/env.h
        src/iterator.h
        src/log_format.h
        src/options.h
        src/random.h
        src/repeat_detector.cpp src/repeat_detector.h
        src/skiplist.h
        src/slice.h
        src/index.cpp src/index.h
        test/coder_test.cpp
        test/skiplist_test.cpp
        test/repeat_detector_test.cpp)

add_executable(LeviDB ${SOURCE_FILES})