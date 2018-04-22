cmake_minimum_required(VERSION 3.8)
project(levidb)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

add_executable(levidb main.cpp
        )