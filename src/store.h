#pragma once
#ifndef LEVIDB_STORE_H
#define LEVIDB_STORE_H

/*
 * logream 封装
 */

#include <memory>

#include "../include/slice.h"

namespace levidb {
    class Store {
    public:
        Store() = default;

        virtual ~Store() = default;

    public:
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
