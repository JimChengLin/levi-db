#pragma once
#ifndef LEVIDB8_EXCEPTION_H
#define LEVIDB8_EXCEPTION_H

/*
 * 不可控异常
 */

#include <exception>
#include <memory>

#include "slice.h"

namespace levidb8 {
    class Exception : public std::exception {
    private:
        enum Code {
            NOT_FOUND = 1,
            CORRUPTION = 2,
            NOT_SUPPORTED = 3,
            INVALID_ARGUMENT = 4,
            IO_ERROR = 5,
        };
        std::unique_ptr<char[]> _state;

    public:
        const char * what() const noexcept override { return "LeviDB8Exception"; }

        Exception(Exception &&) noexcept = default;

        ~Exception() noexcept override = default;

    public:
        bool isNotFound() const noexcept { return code() == NOT_FOUND; }

        bool isCorruption() const noexcept { return code() == CORRUPTION; }

        bool isIOError() const noexcept { return code() == IO_ERROR; }

        bool isNotSupportedError() const noexcept { return code() == NOT_SUPPORTED; }

        bool isInvalidArgument() const noexcept { return code() == INVALID_ARGUMENT; }

        std::string toString() const noexcept;

    public:
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

    private:
        Code code() const noexcept { return static_cast<Code>(_state[4]); }

        Exception(Code code, const Slice & msg, const Slice & msg2) noexcept;
    };
}

#endif //LEVIDB8_EXCEPTION_H
