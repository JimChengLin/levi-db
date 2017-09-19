#ifndef LEVIDB_COMPACT_H
#define LEVIDB_COMPACT_H

/*
 * CompactingDB 替换 compact 中的 DB
 * 使得外界感受不到变化
 * 同时保证崩溃后可恢复
 */

#include <atomic>

#include "../db.h"

namespace LeviDB {
    class CompactingDB : public DB {
    protected:
        std::atomic<bool> _compacting{false};

    public:
        using DB::DB;

        DELETE_MOVE(CompactingDB);
        DELETE_COPY(CompactingDB);

        ~CompactingDB() noexcept override { assert(!_compacting); }
    };
}

#endif //LEVIDB_COMPACT_H