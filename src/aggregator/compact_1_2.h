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
        SeqGenerator * _seq_gen;
        std::unique_ptr<DB> _resource;
        std::unique_ptr<DB> _product_a;
        std::unique_ptr<DB> _product_b;
        std::exception_ptr _e_a;
        std::exception_ptr _e_b;
        std::string _a_end;
        std::string _b_end;

        std::set<std::string, SliceComparator> _ignore;
        ReadWriteLock _rw_lock;
        ReadWriteLock _a_lock;
        ReadWriteLock _b_lock;

        std::atomic<bool> _e_a_bool{false};
        std::atomic<bool> _e_b_bool{false};
        std::atomic<bool> _a_end_meet{false};
        std::atomic<bool> _b_end_meet{false};

    public:
        Compacting1To2DB(std::unique_ptr<DB> && resource, SeqGenerator * seq_gen);
        DELETE_MOVE(Compacting1To2DB);
        DELETE_COPY(Compacting1To2DB);

        EXPOSE(_compacting);

        EXPOSE(_product_a);

        EXPOSE(_product_b);

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

        void updateKeyRange() override;

        bool explicitRemove(const WriteOptions & options,
                            const Slice & key) override;

        void sync() override;
    };

    typedef std::function<void(const Exception &)> reporter_t;

    bool repairCompacting1To2DB(const std::string & db_name, reporter_t reporter) noexcept;
}

#endif //LEVIDB_ONE_TO_TWO_COMPACT_H