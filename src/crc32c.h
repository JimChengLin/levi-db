#ifndef LEVIDB_CRC32C_H
#define LEVIDB_CRC32C_H

/*
 * CRC32C 计算
 *
 * steal from leveldb
 */

#include <cstdint>
#include <cstddef>

namespace LeviDB {
    namespace CRC32C {
        uint32_t extend(uint32_t init_crc, const char * data, size_t n) noexcept;

        inline uint32_t value(const char * data, size_t n) noexcept { return extend(0, data, n); }

        bool verify(const char * data, size_t n, const uint8_t * checksum) noexcept;
    }
}

#endif //LEVIDB_CRC32C_H