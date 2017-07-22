#ifndef LEVIDB_CODING_H
#define LEVIDB_CODING_H

/*
 * 变长 uint_t
 */

#include <cstdint>

namespace LeviDB {
    namespace Coding {
        char * encodeVarint32(char * dst, uint32_t value) noexcept;

        const char * // nullptr when fail
        decodeVarint32(const char * p, const char * limit, uint32_t * value) noexcept;
    }
}

#endif //LEVIDB_CODING_H