#pragma once
#ifndef LEVIDB8_VARINT_H
#define LEVIDB8_VARINT_H

/*
 * 变长 uint32
 */

#include <cstdint>

namespace levidb8 {
    char * encodeVarint32(char * dst, uint32_t value) noexcept;

    const char * // nullptr if fail
    decodeVarint32(const char * p, const char * limit, uint32_t * value) noexcept;
}

#endif //LEVIDB8_VARINT_H
