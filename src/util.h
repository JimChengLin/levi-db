#ifndef LEVIDB_UTIL_H
#define LEVIDB_UTIL_H

#include <cstdint>

namespace LeviDB {
    static_assert(sizeof(uint8_t) == sizeof(char), "uint8_t != char");

    inline uint8_t char_be_uint8(char c) {
        return *reinterpret_cast<uint8_t *>(&c);
    }

    inline char uint8_be_char(uint8_t i) {
        return *reinterpret_cast<char *>(&i);
    }
}

#endif //LEVIDB_UTIL_H