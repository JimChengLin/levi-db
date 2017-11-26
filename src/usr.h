#pragma once
#ifndef LEVIDB8_USR_H
#define LEVIDB8_USR_H

#include "slice.h"
#include "util.h"

namespace levidb8 {
    class UniversalStringRepresentation {
    private:
        std::string _src;
        std::string _extra;

    public:
        // default
        UniversalStringRepresentation() noexcept : UniversalStringRepresentation(std::string()) {}

        // copy/move
        UniversalStringRepresentation(std::string src) noexcept
                : _src(std::move(src)), _extra(_src.size(), uint8ToChar(UINT8_MAX)) {}

    public:
        EXPOSE(_src);

        EXPOSE(_extra);

        void reveal(size_t idx, char mask, bool bit) noexcept;

        void clear() noexcept;

        Slice toSlice() const noexcept { return {_src}; }
    };

    using USR = UniversalStringRepresentation;
}

#endif //LEVIDB8_USR_H
