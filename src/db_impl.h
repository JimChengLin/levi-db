#pragma once
#ifndef LEVIDB_DB_IMPL_H
#define LEVIDB_DB_IMPL_H

#include <atomic>

#include "../include/db.h"
#include "concurrent_index.h"

namespace levidb {
    struct StoreInfo {
        bool compress;
    };

    struct open_t {
    };

    struct reopen_t {
    };

    struct repair_t {
    };

    class DBImpl : public DB {
    private:
        const std::string name_;
        const OpenOptions options_;

        std::atomic<size_t> seq_;
        std::vector<std::vector<size_t>> stores_;
        std::unordered_map<size_t, StoreInfo> stores_map_;

        StoreManager manager_;
        ConcurrentIndex index_;

    public:
        DBImpl(const std::string & name,
               const OpenOptions & options,
               open_t);

        DBImpl(const std::string & name,
               const OpenOptions & options,
               reopen_t);

        DBImpl(const std::string & name,
               const OpenOptions & options,
               repair_t);

        ~DBImpl() override;

    public:
        bool Get(const Slice & k, std::string * v) const override;

        std::unique_ptr<Iterator>
        GetIterator() const override;

        void Add(const Slice & k, const Slice & v) override;

        void Del(const Slice & k) override;

        bool Compact() override;

        void Sync() override;

    private:
        const std::string & GetName() const { return name_; }

        size_t GetLv(size_t seq) const;

        bool IsCompressed(size_t seq) const;

        size_t UniqueSeq();

        void Register(size_t seq);

        friend class StoreManager;

    private:
        std::vector<std::unique_ptr<Index>>
        OpenIndexes();

        std::vector<std::unique_ptr<Index>>
        ReopenIndexes();

        void LoadOrSetInitInfo();
    };
}

#endif //LEVIDB_DB_IMPL_H
