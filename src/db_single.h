#ifndef LEVIDB_SINGLE_DB_H
#define LEVIDB_SINGLE_DB_H

/*
 * 实现 DB 接口的数据库分片
 *
 * 要求:
 * 1. index 和 data 文件最大 4GB, 超过之后的行为 UB
 * 2. 线程安全由读写锁保证
 * 3. 除实现 DB 接口要求外, 还要能提供当前覆盖的 key 的范围以及各文件大小
 * 4. 只要 data 文件不损坏, 必能恢复 keeper 和 index 文件
 *
 * 分片存储于"{name}"文件夹
 * 其中包含以下文件:
 * 1. {name}.data
 * 2. {name}.index
 * 3. {name}.keeper
 * 4. {name}.lock
 */

#include "db.h"
#include "env_thread.h"
#include "index_iter_regex.h"
#include "log_writer.h"
#include "meta_keeper.h"
#include "optional.h"

namespace LeviDB {
    /*
     * 格式:
     * from_k_len + to_k_len + trailing
     * 从 trailing 中切出 key 的区间
     */
    struct DBSingleWeakMeta {
        OffsetToEmpty _offset{IndexConst::disk_null_};
        uint32_t from_k_len = 0;
        uint32_t to_k_len = 0;
    };

    class DBSingle : public DB {
    private:
        SeqGenerator * _seq_gen;
        Optional<FileLock> _file_lock;
        Optional<AppendableFile> _af;
        Optional<RandomAccessFile> _rf;
        Optional<IndexRegex> _index;
        Optional<LogWriter> _writer;
        Optional<WeakKeeper<DBSingleWeakMeta>> _meta;
        ReadWriteLock _rwlock;

    public:
        DBSingle(std::string name, Options options, SeqGenerator * seq_gen);
        DELETE_MOVE(DBSingle);
        DELETE_COPY(DBSingle);

    public:
        ~DBSingle() noexcept override = default;

        void put(const WriteOptions & options,
                 const Slice & key,
                 const Slice & value) override;

        void remove(const WriteOptions & options,
                    const Slice & key) override;

        void write(const WriteOptions & options,
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

        uint64_t indexFileSize() const noexcept;

        uint64_t dataFileSize() const noexcept;

    private:
        void explicitRemove(const WriteOptions & options, const Slice & key);

        void simpleRepair() noexcept;

        Slice largestKey() const noexcept;

        Slice smallestKey() const noexcept;

        void updateKeyRange(const Slice & key) noexcept;
    };
}

#endif //LEVIDB_SINGLE_DB_H