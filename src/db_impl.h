#pragma once
#ifndef LEVIDB_DB_IMPL_H
#define LEVIDB_DB_IMPL_H

#include "../include/db.h"

namespace levidb {
    class DBImpl : public DB {
    public:
        size_t GetLv(size_t seq) const;

        bool IsCompressed(size_t seq) const;

        size_t UniqueSeq();

        void RegisterStore(size_t seq);
    };
}

#endif //LEVIDB_DB_IMPL_H
