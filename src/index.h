#pragma once
#ifndef LEVIDB_INDEX_H
#define LEVIDB_INDEX_H

/*
 * sig_tree 封装
 */

#include <memory>

#include "../include/iterator.h"
#include "store_manager.h"

namespace levidb {
    class Index {
    public:
        Index() = default;

        virtual ~Index() = default;

    public:
        virtual bool Get(const Slice & k, std::string * v) const = 0;

        virtual bool GetInternal(const Slice & k, uint64_t * v) const = 0;

        virtual bool Add(const Slice & k, const Slice & v, bool overwrite) = 0;

        virtual bool AddInternal(const Slice & k, uint64_t v) = 0;

        virtual bool Del(const Slice & k) = 0;

        virtual std::unique_ptr<Iterator>
        GetIterator() const = 0;

    public:
        static std::unique_ptr<Index>
        Open(const std::string & fname, StoreManager * manager);
    };
}

#endif //LEVIDB_INDEX_H
