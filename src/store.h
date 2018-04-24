#pragma once
#ifndef LEVIDB_STORE_H
#define LEVIDB_STORE_H

/*
 * logream 封装
 */

#include <exception>
#include <memory>

#include "../include/slice.h"

namespace levidb {
    class StoreFullException : public std::exception {
    public:
        const char * what() const noexcept override {
            return "no enough space for storage";
        }
    };

    class Store {
    public:
        Store() = default;

        virtual ~Store() = default;

    public:
        enum {
            kMaxSize = 2 * 1024 * 1024 * 1024
        };

        virtual size_t Add(const Slice & s, bool sync) = 0;

        virtual size_t Get(size_t id, std::string * s) const = 0;

    public:
        static std::unique_ptr<Store>
        OpenForSequentialRead(const std::string & fname);

        static std::unique_ptr<Store>
        OpenForRandomRead(const std::string & fname);

        static std::unique_ptr<Store>
        OpenForReadWrite(const std::string & fname);

        static std::unique_ptr<Store>
        OpenForCompressedWrite(const std::string & fname);
    };
}

#endif //LEVIDB_STORE_H
