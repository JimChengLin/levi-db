find_path(ROCKSDB_INCLUDE_DIR
        NAMES rocksdb/db.h
        HINTS ${ROCKSDB_ROOT_DIR}/include/)

find_library(ROCKSDB_LIBRARIES
        NAMES rocksdb
        HINTS ${ROCKSDB_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(rocksdb DEFAULT_MSG ROCKSDB_LIBRARIES ROCKSDB_INCLUDE_DIR)

mark_as_advanced(
        ROCKSDB_LIBRARIES
        ROCKSDB_INCLUDE_DIR)