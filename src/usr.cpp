#include "usr.h"

namespace levidb8 {
    // mask e.g. 0b0100_0000
    void UniversalStringRepresentation::reveal(size_t idx, char mask, bool bit, uint8_t n) noexcept {
        assert(mask != 0);
        const size_t size = idx + 1;
        _src.resize(size);
        _extra.resize(size);

        _src[idx] ^= (-bit ^ _src[idx]) & mask; // changing the nth bit to x
        _extra[idx] |= mask;

        // __builtin_ffs: returns one plus the index of the least significant 1-bit of x
        // if x is zero, returns zero
        assert(__builtin_ffs(mask) - 1 == n);
        mask = uint8ToChar(static_cast<uint8_t>(UINT8_MAX) << n);
        _src[idx] &= mask;
        _extra[idx] &= mask;
    }

    void UniversalStringRepresentation::clear() noexcept {
        _src.clear();
        _extra.clear();
    }
}