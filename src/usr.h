#pragma once
#ifndef LEVIDB8_USR_H
#define LEVIDB8_USR_H

#include "../include/slice.h"
#include "util.h"

namespace levidb8 {
    class UniversalStringRepresentation {
    private:
        std::string _src;
        std::string _extra;

    public:
        UniversalStringRepresentation() noexcept : _src(1, static_cast<char>(0)),
                                                   _extra(1, static_cast<char>(0)) {};

        UniversalStringRepresentation(std::string src) noexcept
                : _src(std::move(src)), _extra(_src.size(), uint8ToChar(UINT8_MAX)) {}

    public:
        EXPOSE(_src);

        EXPOSE(_extra);

        void reveal(size_t idx, char mask, bool bit, uint8_t n) noexcept;

        void clear() noexcept;

        Slice toSlice() const noexcept { return {_src}; }
    };

    using USR = UniversalStringRepresentation;
}

#endif //LEVIDB8_USR_H
