#pragma once
#ifndef LEVIDB8_UTIL_H
#define LEVIDB8_UTIL_H

#include <cstdint>

#define EXPOSE(field) \
inline auto & mut ## field() noexcept { return field; } \
inline const auto & immut ## field() const noexcept { return field; }

namespace levidb8 {
    static_assert(sizeof(uint8_t) == sizeof(char), "cannot reinterpret safely");

    inline uint8_t charToUint8(char c) noexcept {
        return *reinterpret_cast<uint8_t *>(&c);
    }

    inline char uint8ToChar(uint8_t i) noexcept {
        return *reinterpret_cast<char *>(&i);
    }
}

#endif //LEVIDB8_UTIL_H
