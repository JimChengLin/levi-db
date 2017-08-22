#ifndef LEVIDB_VARINT_H
#define LEVIDB_VARINT_H

/*
 * 变长 uint
 */

#include <cstdint>

namespace LeviDB {
    char * encodeVarint32(char * dst, uint32_t value) noexcept;

    const char * // nullptr if fail
    decodeVarint32(const char * p, const char * limit, uint32_t * value) noexcept;
}

#endif //LEVIDB_VARINT_H