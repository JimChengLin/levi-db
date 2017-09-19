#ifndef LEVIDB_DB_H
#define LEVIDB_DB_H

/*
 * DB 接口
 * 要求:
 * 1. 实现增删改, 随机迭代器, (正向|反向)正则匹配迭代器
 * 2. 线程安全
 * 3. 妥善处理异常, 崩溃后不需要额外操作进行数据恢复
 * 4. 文件级自省, 提供数据库路径即可打开
 */

#include <functional>
#include <string>

#include "exception.h"
#include "iterator.h"
#include "levi_regex/r.h"
#include "options.h"
#include "slice.h"

namespace LeviDB {
    class DB {
    protected:
        std::string _name;
        Options _options;

    public:
        DB(std::string name, Options options) noexcept : _name(std::move(name)), _options(options) {};
        DELETE_MOVE(DB);
        DELETE_COPY(DB);

    public:
        virtual ~DB() noexcept = default;

        virtual void put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) = 0;

        virtual void remove(const WriteOptions & options,
                            const Slice & key) = 0;

        virtual void write(const WriteOptions & options,
                           const std::vector<std::pair<Slice, Slice>> & kvs) = 0;

        virtual std::pair<std::string, bool>
        get(const ReadOptions & options, const Slice & key) const = 0;

        virtual std::unique_ptr<Snapshot> makeSnapshot() = 0;

        virtual std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator(std::unique_ptr<Snapshot> && snapshot) const = 0;

        virtual std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex, std::unique_ptr<Snapshot> && snapshot) const = 0;

        virtual std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex, std::unique_ptr<Snapshot> && snapshot) const = 0;

        virtual void tryApplyPending() = 0;

        virtual bool canRelease() const = 0;
    };

    typedef std::function<void(const Exception &)> reporter_t;

    bool repairDBSingle(const std::string & db_single_name, reporter_t reporter) noexcept;

    bool repairDB(const std::string & db_name, reporter_t reporter) noexcept;
}

#endif //LEVIDB_DB_H