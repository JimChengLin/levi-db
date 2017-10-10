#include "lv_db.h"

namespace LeviDB {
    void LvDB::put(const WriteOptions & options,
                   const Slice & key,
                   const Slice & value) {
        try {
            _aggregator.put(options, key, value);
        } catch (const Exception & e) {
            Logger::logForMan(_aggregator.mut_logger().get(), "Put: %s", e.toString().c_str());
            throw e;
        }
    }

    void LvDB::remove(const WriteOptions & options,
                      const Slice & key) {
        try {
            _aggregator.remove(options, key);
        } catch (const Exception & e) {
            Logger::logForMan(_aggregator.mut_logger().get(), "Remove: %s", e.toString().c_str());
            throw e;
        }
    }

    void LvDB::write(const WriteOptions & options,
                     const std::vector<std::pair<Slice, Slice>> & kvs) {
        try {
            WriteOptions opt = options;
            if (opt.compress) {
                opt.uncompress_size = 0;
                for (const auto & kv:kvs) {
                    opt.uncompress_size += kv.first.size() + kv.second.size();
                }
            }
            _aggregator.write(opt, kvs);
        } catch (const Exception & e) {
            Logger::logForMan(_aggregator.mut_logger().get(), "Write: %s", e.toString().c_str());
            throw e;
        }
    }

    std::pair<std::string, bool>
    LvDB::get(const ReadOptions & options, const Slice & key) const {
        try {
            return _aggregator.get(options, key);
        } catch (const Exception & e) {
            Logger::logForMan(const_cast<Logger *>(_aggregator.immut_logger().get()),
                              "Get: %s", e.toString().c_str());
            throw e;
        }
    };

    std::unique_ptr<Snapshot>
    LvDB::makeSnapshot() { // too solid to fail
        return _aggregator.makeSnapshot();
    }

    std::unique_ptr<Iterator<Slice, std::string>>
    LvDB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        try {
            return _aggregator.makeIterator(std::move(snapshot));
        } catch (const Exception & e) {
            Logger::logForMan(const_cast<Logger *>(_aggregator.immut_logger().get()),
                              "MakeIterator: %s", e.toString().c_str());
            throw e;
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    LvDB::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                            std::unique_ptr<Snapshot> && snapshot) const {
        try {
            return _aggregator.makeRegexIterator(std::move(regex), std::move(snapshot));
        } catch (const Exception & e) {
            Logger::logForMan(const_cast<Logger *>(_aggregator.immut_logger().get()),
                              "MakeRegexIterator: %s", e.toString().c_str());
            throw e;
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    LvDB::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                    std::unique_ptr<Snapshot> && snapshot) const {
        try {
            return _aggregator.makeRegexReversedIterator(std::move(regex), std::move(snapshot));
        } catch (const Exception & e) {
            Logger::logForMan(const_cast<Logger *>(_aggregator.immut_logger().get()),
                              "MakeRegexReversedIterator: %s", e.toString().c_str());
            throw e;
        }
    };
}