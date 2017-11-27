# - Find leveldb
#
# LEVELDB_INCLUDE_DIR - where to find leveldb.h, etc.
# LEVELDB_LIBRARIES - List of libraries when using leveldb.
# LEVELDB_FOUND - True if leveldb found.

find_path(LEVELDB_INCLUDE_DIR
        NAMES db.h
        HINTS ${LEVELDB_ROOT_DIR}/include/leveldb)

find_library(LEVELDB_LIBRARIES
        NAMES leveldb
        HINTS ${LEVELDB_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(leveldb DEFAULT_MSG LEVELDB_LIBRARIES LEVELDB_INCLUDE_DIR)

mark_as_advanced(
        LEVELDB_LIBRARIES
        LEVELDB_INCLUDE_DIR)