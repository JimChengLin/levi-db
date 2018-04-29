#pragma once
#ifndef LEVIDB_STORE_MANAGER_H
#define LEVIDB_STORE_MANAGER_H

/*
 * Store 缓存层
 * 防止频繁调用 system call - open
 */

#include <memory>
#include <mutex>

#include "lru_cache.h"
#include "store.h"

namespace levidb {
    class DBImpl;

    class StoreManager {
    private:
        enum {
            kMaxEntries = 128
        };

        DBImpl * db_;
        LRUCache<size_t, std::shared_ptr<Store>, kMaxEntries> cache_;
        size_t seq_;
        std::shared_ptr<Store> curr_;
        std::string backup_;
        std::mutex mutex_;

    public:
        StoreManager() : db_(nullptr), seq_(0) {};

        explicit StoreManager(DBImpl * db)
                : db_(db),
                  seq_(0) {}

        StoreManager(const StoreManager &) = delete;

        StoreManager & operator=(const StoreManager &) = delete;

    public:
        std::shared_ptr<Store>
        OpenStoreForRandomRead(size_t seq);

        std::shared_ptr<Store>
        OpenStoreForReadWrite(size_t * seq, std::shared_ptr<Store> prev);
    };
}

#endif //LEVIDB_STORE_MANAGER_H
