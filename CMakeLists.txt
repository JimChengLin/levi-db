cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)
add_compile_options(-DOS_MACOSX) # 主要在 Mac 上开发

set(SOURCE_FILES
        main.cpp
        src/io_env.cpp src/thread_env.cpp src/env.h
        src/slice.h
        src/exception.cpp src/exception.h src/iterator.h
        src/arena.cpp src/arena.h
        src/log_format.h)

add_executable(LeviDB ${SOURCE_FILES})