#include "usr.h"

namespace levidb8 {
    // mask e.g. 0b1011_1111
    void UniversalStringRepresentation::reveal(size_t idx, char mask, bool bit) noexcept {
        assert(mask != 0);
        const size_t size = idx + 1;
        _src.resize(size);
        _extra.resize(size);

        char inverse_mask = ~mask; // 0b0100_0000
        _src[idx] ^= (-bit ^ _src[idx]) & inverse_mask; // changing the nth bit to x
        _extra[idx] |= inverse_mask;

        // __builtin_ffs: returns one plus the index of the least significant 1-bit of x
        // if x is zero, returns zero
        auto n = __builtin_ffs(inverse_mask);
        mask = uint8ToChar(UINT8_MAX >> (n - 1) << (n - 1));
        _src[idx] &= mask;
        _extra[idx] &= mask;
    }

    void UniversalStringRepresentation::clear() noexcept {
        _src.clear();
        _extra.clear();
    }
}