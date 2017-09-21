#ifndef LEVIDB_ONE_TO_TWO_COMPACT_H
#define LEVIDB_ONE_TO_TWO_COMPACT_H

/*
 * 将一个数据库分片 compact 成两个
 */

#include <exception>
#include <mutex>
#include <set>

#include "../db_single.h"
#include "compact.h"

namespace LeviDB {
    class Compacting1To2DB : public CompactingDB {
    private:
        std::unique_ptr<const DB> _resource;
        std::unique_ptr<DB> _product_a;
        std::unique_ptr<DB> _product_b;
        std::exception_ptr _e_a;
        std::exception_ptr _e_b;
        std::string _a_end;
        std::string _b_end;

        std::set<std::string, SliceComparator> _ignore;
        mutable std::mutex _lock;

    public:
        Compacting1To2DB(std::unique_ptr<DB> && resource, SeqGenerator * seq_gen) noexcept;
        DELETE_MOVE(Compacting1To2DB);
        DELETE_COPY(Compacting1To2DB);

    public:
        ~Compacting1To2DB() noexcept override;

        bool put(const WriteOptions & options,
                 const Slice & key,
                 const Slice & value) override;

        bool remove(const WriteOptions & options,
                    const Slice & key) override;

        bool write(const WriteOptions & options,
                   const std::vector<std::pair<Slice, Slice>> & kvs) override;

        std::pair<std::string, bool>
        get(const ReadOptions & options,
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

        void tryApplyPending() override;

        bool canRelease() const override;

        Slice largestKey() const override;

        Slice smallestKey() const override;

    private:
        // methods below are protected by mutex
        void reportIgnoredKey(const Slice & k) noexcept;

        bool isKeyIgnored(const Slice & k) const noexcept;
    };
}

#endif //LEVIDB_ONE_TO_TWO_COMPACT_H