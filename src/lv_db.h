#ifndef LEVIDB_LV_DB_H
#define LEVIDB_LV_DB_H

/*
 * 外部使用者的 DB
 */

#include "aggregator/aggregator.h"

namespace LeviDB {
    class LvDB {
    private:
        Aggregator _aggregator;

    public:
        LvDB(std::string name, Options options) : _aggregator(std::move(name), options) {}

        DELETE_MOVE(LvDB);
        DELETE_COPY(LvDB);

        ~LvDB() noexcept = default;

    public:
        void put(const WriteOptions & options,
                 const Slice & key,
                 const Slice & value);

        void remove(const WriteOptions & options,
                    const Slice & key);

        void write(const WriteOptions & options,
                   const std::vector<std::pair<Slice, Slice>> & kvs);

        std::pair<std::string, bool>
        get(const ReadOptions & options, const Slice & key) const;

        std::unique_ptr<Snapshot>
        makeSnapshot();

        std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator(std::unique_ptr<Snapshot> && snapshot) const;

        // TODO: 作者对于 Regex 采用朴素 NFA 实现, 有严重性能问题, 禁止使用!!!
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex,
                          std::unique_ptr<Snapshot> && snapshot) const;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const;

        std::string getProperty() const noexcept; // mainly for debug
    };
}

#endif //LEVIDB_LV_DB_H