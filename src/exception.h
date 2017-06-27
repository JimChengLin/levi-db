#ifndef LEVIDB_EXCEPTION_H
#define LEVIDB_EXCEPTION_H

/*
 * 从 leveldb 的 Status 改写而来
 * 全项目的异常(不包含 STL)都在这里
 */

#include "slice.h"
#include <exception>
#include <memory>

namespace LeviDB {
    class Exception : public std::exception {
    public:
        Exception() noexcept : _state(nullptr) {}

        ~Exception() noexcept {}

        virtual const char * what() const override {
            return Exception::_what;
        }

        // 允许复制
        Exception(const Exception & e) noexcept;

        inline void operator=(const Exception & e) noexcept;

        static Exception notFoundException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(NOT_FOUND, msg, msg2);
        }

        static Exception corruptionException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(CORRUPTION, msg, msg2);
        }

        static Exception notSupportedException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(NOT_SUPPORTED, msg, msg2);
        }

        static Exception invalidArgumentException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(INVALID_ARGUMENT, msg, msg2);
        }

        static Exception IOErrorException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(IO_ERROR, msg, msg2);
        }

        bool isNotFound() const noexcept { return code() == NOT_FOUND; }

        bool isCorruption() const noexcept { return code() == CORRUPTION; }

        bool isIOError() const noexcept { return code() == IO_ERROR; }

        bool isNotSupportedError() const noexcept { return code() == NOT_SUPPORTED; }

        bool isInvalidArgument() const noexcept { return code() == INVALID_ARGUMENT; }

        std::string toString() const noexcept;

    private:
        static constexpr char _what[] = "LeviDBException";
        std::unique_ptr<char[]> _state;

        enum Code {
            NOT_FOUND = 1,
            CORRUPTION = 2,
            NOT_SUPPORTED = 3,
            INVALID_ARGUMENT = 4,
            IO_ERROR = 5,
        };

        Code code() const noexcept {
            assert(_state != nullptr);
            return static_cast<Code>(_state[4]);
        }

        Exception(Code code, const Slice & msg, const Slice & msg2) noexcept;

        static std::unique_ptr<char[]> copyState(const char * s) noexcept;
    };

    // 复制构造
    Exception::Exception(const Exception & e) noexcept
            : _state((e._state == nullptr) ? nullptr : copyState(e._state.get())) {
    }

    inline void Exception::operator=(const Exception & e) noexcept {
        if (_state != e._state) {
            _state = (e._state == nullptr) ? nullptr : copyState(e._state.get());
        }
    }
}

#endif //LEVIDB_EXCEPTION_H
