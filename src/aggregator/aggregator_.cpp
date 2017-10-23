#include "../db_single.h"
#include "aggregator.h"

namespace LeviDB {
    class Aggregator::ChainIterator : public Iterator<Slice, std::string> {
    private:
        Aggregator * _aggregator;
        std::unique_ptr<Snapshot> _snapshot;
        std::unique_ptr<Iterator<Slice, std::string>> _iter;
        std::string _bound;

    public:
        ChainIterator(const Aggregator * aggregator, std::unique_ptr<Snapshot> && snapshot) noexcept
                : _aggregator(const_cast<Aggregator *>(aggregator)), _snapshot(std::move(snapshot)) {}
        DELETE_MOVE(ChainIterator);
        DELETE_COPY(ChainIterator);

    public:
        ~ChainIterator() noexcept override = default;

        bool valid() const override {
            return _iter != nullptr && _iter->valid();
        }

        void seekToFirst() override {
            seekTo<true>();
        }

        void seekToLast() override {
            seekTo<false>();
        }

        void seek(const Slice & target) override {
            {
                RWLockReadGuard read_guard;
                auto match = _aggregator->findBestMatchForRead(target, &read_guard, &_bound);
                if (match->db == nullptr) {
                    {
                        RWLockReadGuard _(std::move(read_guard));
                    }
                    RWLockWriteGuard write_guard;
                    match = _aggregator->findBestMatchForWrite(target, &write_guard, &_bound);
                    if (match->db == nullptr) {
                        match->db = std::make_unique<DBSingle>(match->db_name, Options{}, &_aggregator->_seq_gen);
                        ++_aggregator->_operating_dbs;
                    }
                    _iter = match->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                } else {
                    _iter = match->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                }
            }
            _iter->seek(target);
            mayNextUtilValidOrEnd();
        }

        void next() override {
            assert(valid());
            _iter->next();
            mayNextUtilValidOrEnd();
        }

        void prev() override {
            assert(valid());
            _iter->prev();
            mayPrevUtilValidOrBegin();
        }

        Slice key() const override {
            assert(valid());
            return _iter->key();
        }

        std::string value() const override {
            assert(valid());
            return _iter->value();
        }

    private:
        template<bool TRUE_FIRST_FALSE_LAST>
        void seekTo() {
            {
                std::shared_ptr<AggregatorNode> res;
                load:
                {
                    RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                    auto find_res = TRUE_FIRST_FALSE_LAST ? _aggregator->_dispatcher.begin()
                                                          : --_aggregator->_dispatcher.end();
                    _bound = find_res->first;
                    res = find_res->second;
                }
                RWLockReadGuard read_guard(res->lock);
                if (res->dirty) { goto load; }

                if (res->db == nullptr) {
                    {
                        RWLockReadGuard _(std::move(read_guard));
                    }
                    load2:
                    {
                        RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                        auto find_res = TRUE_FIRST_FALSE_LAST ? _aggregator->_dispatcher.begin()
                                                              : --_aggregator->_dispatcher.end();
                        _bound = find_res->first;
                        res = find_res->second;
                    }
                    RWLockWriteGuard write_guard(res->lock);
                    if (res->dirty) { goto load2; }

                    if (res->db == nullptr) {
                        res->db = std::make_unique<DBSingle>(res->db_name, Options{}, &_aggregator->_seq_gen);
                        ++_aggregator->_operating_dbs;
                    }
                    _iter = res->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                } else {
                    _iter = res->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                }
            }
            TRUE_FIRST_FALSE_LAST ? (_iter->seekToFirst(), mayNextUtilValidOrEnd())
                                  : (_iter->seekToLast(), mayPrevUtilValidOrBegin());
        }

        template<bool TRUE_NEXT_FALSE_PREV>
        void trySlipToValid() {
            while (!_iter->valid()) {
                std::string bound;
                {
                    RWLockReadGuard read_guard;
                    auto match = TRUE_NEXT_FALSE_PREV ? _aggregator->findNextOfBestMatchForRead(_bound, &read_guard,
                                                                                                &bound)
                                                      : _aggregator->findPrevOfBestMatchForRead(_bound, &read_guard,
                                                                                                &bound);
                    if (match == nullptr) {
                        break;
                    }

                    if (match->db == nullptr) {
                        {
                            RWLockReadGuard _(std::move(read_guard));
                        }
                        RWLockWriteGuard write_guard;
                        match = TRUE_NEXT_FALSE_PREV ? _aggregator->findNextOfBestMatchForWrite(_bound, &write_guard,
                                                                                                &bound)
                                                     : _aggregator->findPrevOfBestMatchForWrite(_bound, &write_guard,
                                                                                                &bound);
                        if (match->db == nullptr) {
                            match->db = std::make_unique<DBSingle>(match->db_name, Options{}, &_aggregator->_seq_gen);
                            ++_aggregator->_operating_dbs;
                        }
                        _iter = match->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                    } else {
                        _iter = match->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                    }
                }
                TRUE_NEXT_FALSE_PREV ? _iter->seekToFirst() : _iter->seekToLast();
                _bound = std::move(bound);
            }
        }

        void mayNextUtilValidOrEnd() {
            trySlipToValid<true>();
        }

        void mayPrevUtilValidOrBegin() {
            trySlipToValid<false>();
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Aggregator::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainIterator>(this, std::move(snapshot));
    };

    static bool regexPossible(Regex::R * regex, DB * db) {
        Slice smallest = db->smallestKey();
        Slice largest = db->largestKey();

        size_t i = 0;
        size_t limit = std::min(smallest.size(), largest.size());
        while (i < limit && smallest[i] == largest[i]) { ++i; }
        return i == 0 || regex->possible(std::string(smallest.data(), smallest.data() + i));
    }

    class Aggregator::ChainRegexIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        Aggregator * _aggregator;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _iter;
        std::string _bound;

    public:
        ChainRegexIterator(const Aggregator * aggregator,
                           std::shared_ptr<Regex::R> regex,
                           std::unique_ptr<Snapshot> && snapshot)
                : _aggregator(const_cast<Aggregator *>(aggregator)),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)) {
            init();
            trySlipToValid();
        }
        DELETE_MOVE(ChainRegexIterator);
        DELETE_COPY(ChainRegexIterator);

    public:
        ~ChainRegexIterator() noexcept override = default;

        bool valid() const override {
            return _iter != nullptr && _iter->valid();
        }

        void next() override {
            assert(valid());
            _iter->next();
            trySlipToValid();
        }

        std::pair<Slice, std::string>
        item() const override {
            assert(valid());
            return _iter->item();
        };

    private:
        void init() {
            std::shared_ptr<AggregatorNode> res;
            load:
            {
                RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                auto find_res = _aggregator->_dispatcher.begin();
                _bound = find_res->first;
                res = find_res->second;
            }
            RWLockReadGuard read_guard(res->lock);
            if (res->dirty) { goto load; }

            if (res->db == nullptr) {
                {
                    RWLockReadGuard _(std::move(read_guard));
                }
                load2:
                {
                    RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                    auto find_res = _aggregator->_dispatcher.begin();
                    _bound = find_res->first;
                    res = find_res->second;
                }
                if (res->dirty) { goto load2; }

                if (res->db == nullptr) {
                    res->db = std::make_unique<DBSingle>(res->db_name, Options{}, &_aggregator->_seq_gen);
                    ++_aggregator->_operating_dbs;
                }
                if (regexPossible(_regex.get(), res->db.get()))
                    _iter = res->db->makeRegexIterator(_regex, std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
            } else {
                if (regexPossible(_regex.get(), res->db.get()))
                    _iter = res->db->makeRegexIterator(_regex, std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
            }
        }

        void trySlipToValid() {
            while (!_iter->valid()) {
                std::string bound;
                {
                    RWLockReadGuard read_guard;
                    auto match = _aggregator->findNextOfBestMatchForRead(_bound, &read_guard, &bound);
                    if (match == nullptr) {
                        break;
                    }

                    if (match->db == nullptr) {
                        {
                            RWLockReadGuard _(std::move(read_guard));
                        }
                        RWLockWriteGuard write_guard;
                        match = _aggregator->findNextOfBestMatchForWrite(_bound, &write_guard, &bound);
                        if (match->db == nullptr) {
                            match->db = std::make_unique<DBSingle>(match->db_name, Options{}, &_aggregator->_seq_gen);
                            ++_aggregator->_operating_dbs;
                        }
                        if (regexPossible(_regex.get(), match->db.get()))
                            _iter = match->db->makeRegexIterator(_regex,
                                                                 std::make_unique<Snapshot>(
                                                                         _snapshot->immut_seq_num()));
                    } else {
                        if (regexPossible(_regex.get(), match->db.get()))
                            _iter = match->db->makeRegexIterator(_regex,
                                                                 std::make_unique<Snapshot>(
                                                                         _snapshot->immut_seq_num()));
                    }
                }
                _bound = std::move(bound);
            }
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainRegexIterator>(this, std::move(regex), std::move(snapshot));
    };

    class Aggregator::ChainRegexReversedIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        Aggregator * _aggregator;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _iter;
        std::string _bound;

    public:
        ChainRegexReversedIterator(const Aggregator * aggregator,
                                   std::shared_ptr<Regex::R> regex,
                                   std::unique_ptr<Snapshot> && snapshot)
                : _aggregator(const_cast<Aggregator *>(aggregator)),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)) {
            init();
            trySlipToValid();
        }
        DELETE_MOVE(ChainRegexReversedIterator);
        DELETE_COPY(ChainRegexReversedIterator);

    public:
        ~ChainRegexReversedIterator() noexcept override = default;

        bool valid() const override {
            return _iter != nullptr && _iter->valid();
        }

        void next() override {
            assert(valid());
            _iter->next();
            trySlipToValid();
        }

        std::pair<Slice, std::string>
        item() const override {
            assert(valid());
            return _iter->item();
        };

    private:
        void init() {
            std::shared_ptr<AggregatorNode> res;
            load:
            {
                RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                auto find_res = --_aggregator->_dispatcher.end();
                _bound = find_res->first;
                res = find_res->second;
            }
            RWLockReadGuard read_guard(res->lock);
            if (res->dirty) { goto load; }

            if (res->db == nullptr) {
                {
                    RWLockReadGuard _(std::move(read_guard));
                }
                load2:
                {
                    RWLockReadGuard dispatcher_guard(_aggregator->_dispatcher_lock);
                    auto find_res = --_aggregator->_dispatcher.end();
                    _bound = find_res->first;
                    res = find_res->second;
                }
                if (res->dirty) { goto load2; }

                if (res->db == nullptr) {
                    res->db = std::make_unique<DBSingle>(res->db_name, Options{}, &_aggregator->_seq_gen);
                    ++_aggregator->_operating_dbs;
                }
                if (regexPossible(_regex.get(), res->db.get()))
                    _iter = res->db->makeRegexReversedIterator(_regex,
                                                               std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
            } else {
                if (regexPossible(_regex.get(), res->db.get()))
                    _iter = res->db->makeRegexReversedIterator(_regex,
                                                               std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
            }
        }

        void trySlipToValid() {
            while (!_iter->valid()) {
                std::string bound;
                RWLockReadGuard read_guard;
                auto match = _aggregator->findPrevOfBestMatchForRead(_bound, &read_guard, &bound);
                if (match == nullptr) {
                    break;
                }

                if (match->db == nullptr) {
                    {
                        RWLockReadGuard _(std::move(read_guard));
                    }
                    RWLockWriteGuard write_guard;
                    match = _aggregator->findPrevOfBestMatchForWrite(_bound, &write_guard, &bound);
                    if (match->db == nullptr) {
                        match->db = std::make_unique<DBSingle>(match->db_name, Options{}, &_aggregator->_seq_gen);
                    }
                    if (regexPossible(_regex.get(), match->db.get()))
                        _iter = match->db->makeRegexReversedIterator(_regex,
                                                                     std::make_unique<Snapshot>(
                                                                             _snapshot->immut_seq_num()));
                } else {
                    if (regexPossible(_regex.get(), match->db.get()))
                        _iter = match->db->makeRegexReversedIterator(_regex,
                                                                     std::make_unique<Snapshot>(
                                                                             _snapshot->immut_seq_num()));
                }
                _bound = std::move(bound);
            }
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                          std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainRegexReversedIterator>(this, std::move(regex), std::move(snapshot));
    };
}