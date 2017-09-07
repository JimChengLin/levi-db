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
        std::string * _src = nullptr;

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

    public:
        EXPOSE(_src);

        void reveal(size_t idx, char mask, bool bit) noexcept;

        void reveal(const Slice & slice) noexcept;

        bool possible(const std::string & input) const noexcept;

        void clear() noexcept;

        Slice toSlice() const noexcept { return {*_src}; }
    };

    using USR = UniversalStringRepresentation;

    class UsrJudge {
    public:
        UsrJudge() noexcept = default;
        DEFAULT_MOVE(UsrJudge);
        DEFAULT_COPY(UsrJudge);

    public:
        virtual ~UsrJudge() noexcept = default;

        virtual bool possible(const USR & input) const = 0;

        virtual bool match(const USR & input) const = 0;
    };
}

#endif //LEVIDB_USR_H