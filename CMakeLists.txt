cmake_minimum_required(VERSION 3.8)
project(levidb)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

if (NOT "${FSANITIZE}" STREQUAL "")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=${FSANITIZE}")
endif ()

if ("$ENV{COVERAGE}" STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

file(GLOB LOGREAM_SOURCE_FILES "logream/src/*.cpp")
file(GLOB POSIX_ENV_SOURCE_FILES "posix_env/src/*.cpp")
include_directories(logream/src)
include_directories(posix_env/src)
include_directories(sig_tree/src)

set(LEVIDB_SOURCE_FILES ${LOGREAM_SOURCE_FILES} ${POSIX_ENV_SOURCE_FILES}
        include/compacter.h
        include/db.h
        include/iterator.h
        include/manifester.h
        include/slice.h
        src/db_impl.cpp src/db_impl.h
        src/file_writer.cpp src/file_writer.h
        src/index.cpp src/index.h
        src/store.cpp src/store.h
        )

add_executable(levidb main.cpp ${LEVIDB_SOURCE_FILES})