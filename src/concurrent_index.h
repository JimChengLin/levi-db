#pragma once
#ifndef LEVIDB_CONCURRENT_INDEX_H
#define LEVIDB_CONCURRENT_INDEX_H

#include <memory>
#include <vector>

#include "index.h"

namespace levidb {
    class ConcurrentIndex {
    private:
        std::vector<std::unique_ptr<Index>> indexes_;

    public:
        explicit ConcurrentIndex(std::vector<std::unique_ptr<Index>> && indexes)
                : indexes_(std::move(indexes)) {}

        ConcurrentIndex(const ConcurrentIndex &) = delete;

        ConcurrentIndex & operator=(const ConcurrentIndex &) = delete;

    public:
        bool Get(const Slice & k, std::string * v) const;

        bool GetInternal(const Slice & k, uint64_t * v) const;

        bool Add(const Slice & k, const Slice & v, bool overwrite);

        bool AddInternal(const Slice & k, uint64_t v);

        bool Del(const Slice & k);

        std::unique_ptr<Iterator>
        GetIterator() const;

        void Sync();

        void RetireStore();

    private:
        static size_t Hash(const Slice & k);
    };
}

#endif //LEVIDB_CONCURRENT_INDEX_H
