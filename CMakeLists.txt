cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)
add_compile_options(-DOS_MACOSX)

set(SOURCE_FILES
        main.cpp
        src/env.cpp src/env.h
        src/slice.h
        src/exception.cpp src/exception.h)

add_executable(LeviDB ${SOURCE_FILES})