#pragma once
#ifndef LEVIDB_STORE_MANAGER_H
#define LEVIDB_STORE_MANAGER_H

/*
 * Store 缓存层
 * 防止频繁调用 system call - open
 */

#include <memory>

#include "store.h"

namespace levidb {
    class DBImpl;

    class StoreManager {
    private:
        DBImpl * db_;

    public:
        std::shared_ptr<Store>
        OpenStoreForRandomRead(size_t seq);

        std::shared_ptr<Store>
        OpenStoreForReadWrite(size_t * seq, std::shared_ptr<Store> prev);
    };
}

#endif //LEVIDB_STORE_MANAGER_H
