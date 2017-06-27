cmake_minimum_required(VERSION 3.7)
project(LeviDB)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES
        main.cpp
        src/env.h
        src/env.cpp
        src/slice.h
        )

add_executable(LeviDB ${SOURCE_FILES})