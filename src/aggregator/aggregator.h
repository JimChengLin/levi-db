#ifndef LEVIDB_AGGREGATOR_H
#define LEVIDB_AGGREGATOR_H

/*
 * Aggregator 管理大量的数据库分片
 * 对外暴露一致的 DB 接口
 * TODO: 这部分设计强烈依赖工程调优, 用户可自行改进
 *
 * Aggregator 物理文件结构:
 * 1. /1/, /2/... - DBSingle 分片
 * 2. log_prev.txt, log.txt - 人类可读日志
 * 3. keeper_a, keeper_b - double buffer 的 meta keeper
 * 4. lock
 */

#include <map>

#include "../db.h"
#include "../env_io.h"
#include "../env_thread.h"
#include "../meta_keeper.h"
#include "../optional.h"

namespace LeviDB {
    namespace AggregatorConst {
        static constexpr int max_dbs_ = 100;
        static constexpr int merge_threshold_ = 128 * 1024;
    }

    struct AggregatorNode {
        std::atomic<size_t> hit{0};
        std::string db_name;
        std::unique_ptr<DB> db;
        ReadWriteLock lock;
        std::atomic<bool> dirty{false};
    };

    struct AggregatorStrongMeta {
        uint64_t counter = 1; // there must be a DBSingle named '0', so starts from 1

        // 程序变动时 +1
        const uint64_t db_version = 1;
        const uint64_t format_version = 1;
    };

    class Aggregator : public DB {
    private:
        SeqGenerator _seq_gen;

        std::map<std::string/* lower_bound */, std::shared_ptr<AggregatorNode>, SliceComparator> _dispatcher;
        ReadWriteLock _dispatcher_lock;

        // @formatter:off
        Optional<FileLock> _file_lock;
        Optional<StrongKeeper<AggregatorStrongMeta>> _meta;
        Optional<Logger> _logger;
        // @formatter:on

        std::mutex _mutex;
        std::atomic<unsigned> _operating_dbs{0};
        std::atomic<bool> _gc{false};

        class ChainIterator;

        class ChainRegexIterator;

        class ChainRegexReversedIterator;

    public:
        Aggregator(std::string name, Options options);
        DELETE_MOVE(Aggregator);
        DELETE_COPY(Aggregator);

        EXPOSE(_logger);

    public:
        ~Aggregator() noexcept override;

        bool put(const WriteOptions & options,
                 const Slice & key,
                 const Slice & value) override;

        bool remove(const WriteOptions & options,
                    const Slice & key) override;

        bool write(const WriteOptions & options,
                   const std::vector<std::pair<Slice, Slice>> & kvs) override;

        std::pair<std::string, bool>
        get(const ReadOptions & options, const Slice & key) const override;

        std::unique_ptr<Snapshot>
        makeSnapshot() override;

        // @formatter:off
        std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator(std::unique_ptr<Snapshot> && snapshot) const override;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex,
                          std::unique_ptr<Snapshot> && snapshot) const override;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const override;
        // @formatter:on

        // 大规模分片时, 以下方法耗费性能或没有实际意义
        [[noreturn]] bool explicitRemove(const WriteOptions & options,
                                         const Slice & key) override { // 已有 remove
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] void tryApplyPending() override { // 析构时自动 apply
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] bool canRelease() const override { // 调用者必须自行保证可以 release
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] Slice largestKey() const override { // 可以用 iter 替代
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] Slice smallestKey() const override { // 可以用 iter 替代
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] void updateKeyRange() override { // 无需让外部手动调用
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] void sync() override { // 设置 sync 为 true 替代
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

    private:
        std::shared_ptr<AggregatorNode>
        findBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                              std::string * lower_bound = nullptr);

        std::shared_ptr<AggregatorNode>
        findPrevOfBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                                    std::string * lower_bound = nullptr);

        std::shared_ptr<AggregatorNode>
        findNextOfBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                                    std::string * lower_bound = nullptr);

        const std::shared_ptr<AggregatorNode>
        findBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                             std::string * lower_bound = nullptr) const;

        const std::shared_ptr<AggregatorNode>
        findPrevOfBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                                   std::string * lower_bound = nullptr) const;

        const std::shared_ptr<AggregatorNode>
        findNextOfBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                                   std::string * lower_bound = nullptr) const;

        void mayOpenDB(std::shared_ptr<AggregatorNode> match);

        std::unique_ptr<DB>
        mayRenameDB(std::unique_ptr<DB> && db);

        // 只有以下两个方法会获得 _dispatcher_lock 的写锁
        void ifCompact1To2Done(std::shared_ptr<AggregatorNode> match);

    public: // for test
        void gc();
    };
}

#endif //LEVIDB_AGGREGATOR_H