#ifndef LEVIDB_AGGREGATOR_H
#define LEVIDB_AGGREGATOR_H

/*
 * Aggregator 管理大量的数据库分片
 * 对外暴露一致的 DB 接口
 * TODO: 这部分设计强烈依赖工程调优, 用户可自行改进
 *
 * Aggregator 物理文件结构:
 * 1. /1/, /2/... - 大量 DBSingle 分片
 * 2. log_prev.txt, log.txt - 人类可读日志
 * 3. keeper_a, keeper_b - double buffer 的 meta keeper
 * 4. lock
 */

#include "../db.h"
#include "../env_io.h"
#include "../env_thread.h"
#include "../meta_keeper.h"
#include "../optional.h"

namespace LeviDB {
    namespace AggregatorConst {
        static constexpr int max_dbs_ = 25;
        static constexpr int merge_threshold_ = 128 * 1024;
    }

    struct AggregatorNode {
        std::unique_ptr<AggregatorNode> next;
        std::unique_ptr<DB> db;
        std::string db_name;
        std::string lower_bound;
        ReadWriteLock lock;
    };

    struct AggregatorStrongMeta {
        uint64_t counter = 0;

        // 程序变动时 +1
        const uint64_t db_version = 1;
        const uint64_t format_version = 1;
    };

    class Aggregator : public DB {
    private:
        SeqGenerator _seq_gen;
        AggregatorNode _head;
        Optional <FileLock> _file_lock;
        Optional <StrongKeeper<AggregatorStrongMeta>> _meta;
        Optional <Logger> _logger;
        std::atomic<bool> _ready_gc{false};

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

        bool explicitRemove(const WriteOptions & options,
                            const Slice & key) override;

        // 大规模分片时, 以下方法浪费性能且没有意义, 不实现
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

        [[noreturn]] void updateKeyRange() override { // 没有意义
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] void sync() override { // 设置 sync 为 true 替代
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

    private:
        void insertNodeUnlocked(std::unique_ptr<AggregatorNode> && node) noexcept;

        std::pair<AggregatorNode *, RWLockWriteGuard>
        findBestMatchForWrite(const Slice & target);

        std::pair<AggregatorNode *, RWLockReadGuard>
        findBestMatchForRead(const Slice & target) const;

        void gc();
    };
}

#endif //LEVIDB_AGGREGATOR_H