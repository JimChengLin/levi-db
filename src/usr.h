#ifndef LEVIDB_USR_H
#define LEVIDB_USR_H

#include <string>

#include "slice.h"
#include "util.h"

namespace LeviDB {
    class UniversalStringRepresentation {
    private:
        std::string _src_;
        std::string _extra;
        std::string * _src;

    public:
        // default
        UniversalStringRepresentation() noexcept : UniversalStringRepresentation("") {}

        // copy/move
        UniversalStringRepresentation(std::string src) noexcept
                : _src_(std::move(src)), _extra(_src_.size(), uint8ToChar(UINT8_MAX)), _src(&_src_) {}

        // share
        UniversalStringRepresentation(std::string * src) noexcept
                : _extra(src->size(), uint8ToChar(UINT8_MAX)), _src(src) {}

        UniversalStringRepresentation(UniversalStringRepresentation && rhs) noexcept;

        UniversalStringRepresentation & operator=(UniversalStringRepresentation && rhs) noexcept;

        DELETE_COPY(UniversalStringRepresentation);

        ~UniversalStringRepresentation() noexcept = default;

        // mask == 0 表示 no-op
        void reveal(size_t idx, char mask) noexcept;

        void reveal(const Slice & slice) noexcept;

        Slice toSlice() const noexcept { return {*_src}; }
    };

    using USR = UniversalStringRepresentation;
}

#endif //LEVIDB_USR_H