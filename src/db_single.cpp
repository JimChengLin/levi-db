#include "db_single.h"

namespace LeviDB {
    void DBSingle::put(const WriteOptions & options,
                       const Slice & key,
                       const Slice & value) {

    };

    void DBSingle::remove(const WriteOptions & options,
                          const Slice & key) {

    };

    void DBSingle::write(const WriteOptions & options,
                         const std::vector<std::pair<Slice, Slice>> & kvs) {

    };

    std::string DBSingle::get(const ReadOptions & options,
                              const Slice & key) const {

    };

    std::unique_ptr<Snapshot>
    DBSingle::makeSnapshot() {

    };

    std::unique_ptr<Iterator<Slice, std::string>>
    DBSingle::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    DBSingle::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    DBSingle::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {

    };
}
