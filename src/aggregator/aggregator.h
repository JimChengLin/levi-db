#ifndef LEVIDB_DB_AGGREGATOR_H
#define LEVIDB_DB_AGGREGATOR_H

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

#include <deque>
#include <map>
#include <unordered_map>

#include "../db.h"
#include "../env_io.h"
#include "../env_thread.h"
#include "../meta_keeper.h"
#include "../optional.h"

namespace LeviDB {
    struct AggregatorStrongMeta {
        uint64_t counter = 0;
        uint64_t file_num = 0;

        // 程序变动时 +1
        const uint64_t db_version = 1;
        const uint64_t format_version = 1;
    };

    class Aggregator : public DB {
    private:
        typedef std::pair<std::mutex, std::condition_variable> sync_pack;

        enum SearchMapCommand {
            SET,
            DEL,
        };

        SeqGenerator seq_gen;
        std::map<std::string/* lower_bound */, std::string/* db_name */, SliceComparator> search_map;
        std::unordered_map<Slice/* dn_name */, std::pair<std::shared_ptr<DB>, sync_pack>, SliceHasher> db_cache;
        std::deque<std::pair<SearchMapCommand, std::pair<std::string, std::string>>> _pending;
        std::mutex _dispatcher_lock;
        Optional<FileLock> _file_lock;
        Optional<Logger> _logger;
        Optional<StrongKeeper<AggregatorStrongMeta>> _meta;

    public:
        Aggregator(std::string name, Options options);
        DELETE_MOVE(Aggregator);
        DELETE_COPY(Aggregator);

    public:
        ~Aggregator() noexcept override = default;

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
}

#endif //LEVIDB_DB_AGGREGATOR_H