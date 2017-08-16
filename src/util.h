#ifndef LEVIDB_UTIL_H
#define LEVIDB_UTIL_H

#include <cstdint>

#define LEVI_STR(x) #x

#define DEFAULT_COPY(cls) \
cls(const cls &) noexcept = default; \
cls & operator=(const cls &) noexcept = default;

#define DELETE_COPY(cls) \
cls(const cls &) noexcept = delete; \
void operator=(const cls &) noexcept = delete;

#define DEFAULT_MOVE(cls) \
cls(cls &&) noexcept = default; \
cls & operator=(cls &&) noexcept = default;

#define DELETE_MOVE(cls) \
cls(cls &&) noexcept = delete; \
void operator=(cls &&) noexcept = delete;

#define EXPOSE(field) \
inline auto & mut ## field() noexcept { return field; } \
inline const auto & immut ##  field() const noexcept { return field; }

namespace LeviDB {
    static_assert(sizeof(uint8_t) == sizeof(char), "cannot reinterpret safely");

    inline uint8_t charToUint8(char c) {
        return *reinterpret_cast<uint8_t *>(&c);
    }

    inline char uint8ToChar(uint8_t i) {
        return *reinterpret_cast<char *>(&i);
    }
}

#endif //LEVIDB_UTIL_H