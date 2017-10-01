#include "aggregator.h"

namespace LeviDB {
    Aggregator::Aggregator(std::string name, Options options)
            : DB(std::move(name), options) {

    };

    bool Aggregator::put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) {

    };

    bool Aggregator::remove(const WriteOptions & options,
                            const Slice & key) {

    };

    bool Aggregator::write(const WriteOptions & options,
                           const std::vector<std::pair<Slice, Slice>> & kvs) {

    };

    std::pair<std::string, bool>
    Aggregator::get(const ReadOptions & options, const Slice & key) const {

    };

    std::unique_ptr<Snapshot>
    Aggregator::makeSnapshot() {

    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Aggregator::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                          std::unique_ptr<Snapshot> && snapshot) const {

    };

    void Aggregator::tryApplyPending() {

    };

    bool Aggregator::canRelease() const {

    };

    Slice Aggregator::largestKey() const {

    };

    Slice Aggregator::smallestKey() const {

    };

    void Aggregator::updateKeyRange() {

    };

    bool Aggregator::explicitRemove(const WriteOptions & options,
                                    const Slice & key) {

    };

    void Aggregator::sync() {

    };
}