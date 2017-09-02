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
    void UniversalStringRepresentation::reveal(size_t idx, char mask) noexcept {
        assert(mask != uint8ToChar(UINT8_MAX));
        size_t size = idx + 1;
        _src->resize(size);
        _extra.resize(size);

        if (mask != 0) { // mask == 0, no information
            char inverse_mask = ~mask; // 0b0100_0000
            (*_src)[idx] |= inverse_mask;
            _extra[idx] |= inverse_mask;

            // clear unknown bits
            while ((mask & 1) == 1) {
                mask >>= 1;
                mask |= 0b10000000;
                (*_src)[idx] &= mask;
                _extra[idx] &= mask;
            }
        }
    };

    void UniversalStringRepresentation::reveal(const Slice & slice) noexcept {
        _src->resize(slice.size());
        _extra.resize(slice.size());

        _src->replace(0, slice.size(), slice.data());
        _extra.replace(0, slice.size(), slice.size(), uint8ToChar(UINT8_MAX));
    }
}