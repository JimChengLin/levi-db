#pragma once
#ifndef LEVIDB_DB_H
#define LEVIDB_DB_H

/*
 * DB 接口
 *
 * 注意:
 * 1. 所有方法 **线程安全**
 * 2. std::unique_ptr<Iterator> **线程不安全**
 * 3. 索引无法区分 "abc\0\0" 与 "abc\0"
 */

#include <memory>

#include "iterator.h"
#include "options.h"

namespace levidb {
    class DB {
    public:
        DB() = default;

        virtual ~DB() = default;

    public:
        virtual bool /* k exists? */
        Get(const Slice & k, std::string * v,
            const GetOptions & options) const = 0;

        virtual std::unique_ptr<Iterator>
        GetIterator(const GetIterOptions & options) const = 0;

        virtual bool /* k exists? */
        Add(const Slice & k, const Slice & v,
            const AddOptions & options) = 0;

        virtual bool /* k exists? */
        Del(const Slice & k,
            const DelOptions & options) = 0;

        virtual bool /* can do more? */
        Compact(const CompactOptions & options) = 0;

        virtual void Sync(const SyncOptions & options) = 0;

    public:
        static std::shared_ptr<DB>
        Open(const std::string & name,
             const OpenOptions & options);
    };
}

#endif //LEVIDB_DB_H
