#ifndef LEVIDB_EXCEPTION_H
#define LEVIDB_EXCEPTION_H

/*
 * 全项目不可控的异常
 *
 * steal from leveldb
 */

#include <exception>
#include <memory>

#include "slice.h"

namespace LeviDB {
    class Exception : public std::exception {
    private:
        std::unique_ptr<char[]> _state;

    public:
        const char * what() const noexcept override { return "LeviDBException"; }

        Exception(Exception &&) noexcept = default;

        Exception & operator=(Exception &&) noexcept = default;

        Exception(const Exception & e) noexcept
                : _state((e._state == nullptr) ? nullptr : copyState(e._state.get())) {}

        Exception & operator=(const Exception & e) noexcept {
            if (_state != e._state) {
                _state = (e._state == nullptr) ? nullptr : copyState(e._state.get());
            }
            return *this;
        }

        ~Exception() noexcept override = default;

        static Exception notFoundException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(NOT_FOUND, msg, msg2);
        }

        // 状态异常
        static Exception corruptionException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(CORRUPTION, msg, msg2);
        }

        static Exception notSupportedException(const Slice & msg, const Slice & msg2 = Slice()) noexcept {
            return Exception(NOT_SUPPORTED, msg, msg2);
        }

        // 用户错误输入
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
}

#endif //LEVIDB_EXCEPTION_H
