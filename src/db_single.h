#ifndef LEVIDB_SINGLE_DB_H
#define LEVIDB_SINGLE_DB_H

/*
 * 实现 DB 接口的数据库分片
 * 分片的 index 和 data 文件最大 4GB
 * 超过之后的行为 UB
 * 由 aggregator 合并分片, 虚拟为一个无容量上限高并发的数据库
 * 分片的线程安全由读写锁保证
 */

#include "db.h"

namespace LeviDB {
    class DBSingle : public DB {
    public:
        DBSingle(std::string name, Options options) noexcept : DB(std::move(name), options) {}
        DELETE_MOVE(DBSingle);
        DELETE_COPY(DBSingle);

    public:
        ~DB() noexcept override = default;

        void put(const WriteOptions & options,
                 const Slice & key,
                 const Slice & value) override;

        void remove(const WriteOptions & options,
                    const Slice & key) override;

        void write(const WriteOptions & options,
                   const std::vector<std::pair<Slice, Slice>> & kvs) override;

        std::string get(const ReadOptions & options,
                        const Slice & key) const override;

        std::unique_ptr<Snapshot>
        makeSnapshot() override;

        std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator(std::unique_ptr<Snapshot> && snapshot) const override;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex,
                          std::unique_ptr<Snapshot> && snapshot) const override;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const override;
    };
}

#endif //LEVIDB_SINGLE_DB_H