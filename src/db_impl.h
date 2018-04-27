#pragma once
#ifndef LEVIDB_DB_IMPL_H
#define LEVIDB_DB_IMPL_H

#include "../include/db.h"

namespace levidb {
    class DBImpl : public DB {
    private:
        size_t GetLv(size_t seq) const;

        bool IsCompressed(size_t seq) const;

        size_t UniqueSeq();

        void Register(size_t seq);

        friend class StoreManager;
    };
}

#endif //LEVIDB_DB_IMPL_H
