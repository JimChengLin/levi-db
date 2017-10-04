#include "aggregator.h"

namespace LeviDB {
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
}