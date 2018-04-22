cmake_minimum_required(VERSION 3.8)
project(levidb)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "$ENV{COVERAGE}" STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

file(GLOB LOGREAM_SOURCE_FILES "logream/src/*.cpp")
file(GLOB POSIX_ENV_SOURCE_FILES "posix_env/src/*.cpp")
file(GLOB SIG_TREE_SOURCE_FILES "sig_tree/src/*.cpp")
include_directories(logream/src)
include_directories(posix_env/src)
include_directories(sig_tree/src)

add_executable(levidb main.cpp ${LOGREAM_SOURCE_FILES} ${POSIX_ENV_SOURCE_FILES} ${SIG_TREE_SOURCE_FILES}
        )