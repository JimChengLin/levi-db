#include <climits>

#include "usr.h"

namespace LeviDB {
    UniversalStringRepresentation::UniversalStringRepresentation(UniversalStringRepresentation && rhs) noexcept {
        operator=(std::move(rhs));
    }

    UniversalStringRepresentation &
    UniversalStringRepresentation::operator=(UniversalStringRepresentation && rhs) noexcept {
        bool ref_self = (_src == &_src_);
        bool rhs_ref_self = (rhs._src == &rhs._src_);

        std::swap(_src_, rhs._src_);
        std::swap(_extra, rhs._extra);
        std::swap(_src, rhs._src);

        if (ref_self) { rhs._src = &rhs._src_; }
        if (rhs_ref_self) { _src = &_src_; }
        return *this;
    }

    // mask e.g. 0b1011_1111
    void UniversalStringRepresentation::reveal(size_t idx, char mask, bool bit) noexcept {
        assert(mask != 0);
        size_t size = idx + 1;
        _src->resize(size);
        _extra.resize(size);

        char inverse_mask = ~mask; // 0b0100_0000
        if (bit) {
            (*_src)[idx] |= inverse_mask;
        } else {
            (*_src)[idx] &= mask;
        }
        _extra[idx] |= inverse_mask;

        // clear unknown bits
        // __builtin_ffs: returns one plus the index of the least significant 1-bit of x
        // if x is zero, returns zero.
        int n = __builtin_ffs(inverse_mask);
        mask = (uint8ToChar(UINT8_MAX) >> (n - 1) << (n - 1));
        (*_src)[idx] &= mask;
        _extra[idx] &= mask;
    };

    void UniversalStringRepresentation::reveal(const Slice & slice) noexcept {
        _src->resize(slice.size());
        _extra.resize(slice.size());

        _src->replace(0, slice.size(), slice.data(), slice.size());
        _extra.replace(0, slice.size(), slice.size(), uint8ToChar(UINT8_MAX));
    }

    bool UniversalStringRepresentation::possible(const std::string & input) const noexcept {
        size_t length = std::min(_src->size(), input.size());
        for (int i = 0; i < length; ++i) {
            if ((_extra[i] & ((*_src)[i] ^ input[i])) != 0) {
                return false;
            }
        }
        return true;
    }

    void UniversalStringRepresentation::clear() noexcept {
        _src->clear();
        _extra.clear();
    }
}