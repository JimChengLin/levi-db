#include "../db_single.h"
#include "aggregator.h"

namespace LeviDB {
    class Aggregator::ChainIterator : public Iterator<Slice, std::string> {
    private:
        Aggregator * _aggregator;
        AggregatorNode * _cursor{};
        std::unique_ptr<Iterator<Slice, std::string>> _iter;
        std::unique_ptr<Snapshot> _snapshot;

    public:
        ChainIterator(const Aggregator * aggregator, std::unique_ptr<Snapshot> && snapshot) noexcept
                : _aggregator(const_cast<Aggregator *>(aggregator)), _snapshot(std::move(snapshot)) {};
        DELETE_MOVE(ChainIterator);
        DELETE_COPY(ChainIterator);

    public:
        ~ChainIterator() noexcept override = default;

        bool valid() const override {
            return _iter != nullptr && _iter->valid();
        };

        void seekToFirst() override {
            _cursor = _aggregator->_head.next.get();
            _iter = mayOpenDBThenMakeIter(_cursor);
            _iter->seekToFirst();
        };

        void seekToLast() override {
            setCursorAndIterBefore(nullptr);
            _iter->seekToLast();
        };

        void seek(const Slice & target) override {
            {
                RWLockReadGuard read_guard;
                _cursor = const_cast<AggregatorNode *>(_aggregator->findBestMatchForRead(target, &read_guard));
                if (_cursor->db == nullptr) {
                    {
                        RWLockReadGuard _(std::move(read_guard));
                    }
                    RWLockWriteGuard write_guard;
                    _cursor = _aggregator->findBestMatchForWrite(target, &write_guard);
                    _cursor->db = std::make_unique<DBSingle>(_cursor->db_name, Options{}, &_aggregator->_seq_gen);
                    _iter = _cursor->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                } else {
                    _iter = _cursor->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                }
            }
            _iter->seek(target);
        };

        void next() override {
            assert(valid());
            _iter->next();
            if (!_iter->valid()) {
                _cursor = _cursor->next.get();
                if (_cursor != nullptr) {
                    _iter = mayOpenDBThenMakeIter(_cursor);
                    _iter->seekToFirst();
                }
            }
        };

        void prev() override {
            assert(valid());
            _iter->prev();
            if (!_iter->valid()) {
                if (setCursorAndIterBefore(_cursor)) {
                    _iter->seekToLast();
                }
            }
        };

        Slice key() const override {
            assert(valid());
            return _iter->key();
        };

        std::string value() const override {
            assert(valid());
            return _iter->value();
        };

    private:
        std::unique_ptr<Iterator<Slice, std::string>>
        mayOpenDBThenMakeIter(AggregatorNode * node) {
            RWLockReadGuard read_guard(node->lock);
            if (node->db == nullptr) {
                {
                    RWLockReadGuard _(std::move(read_guard));
                }
                RWLockWriteGuard write_guard(node->lock);
                node->db = std::make_unique<DBSingle>(node->db_name, Options{}, &_aggregator->_seq_gen);
                return node->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
            }
            return node->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
        }

        bool setCursorAndIterBefore(AggregatorNode * node) {
            AggregatorNode * cursor = &_aggregator->_head;
            AggregatorNode * next{};
            RWLockWriteGuard write_guard;
            while (true) {
                next = cursor->next.get();
                if (next == node) {
                    if (cursor != &_aggregator->_head) {
                        if (cursor->db == nullptr) {
                            cursor->db = std::make_unique<DBSingle>(cursor->db_name, Options{}, &_aggregator->_seq_gen);
                        }
                        _iter = cursor->db->makeIterator(std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                        _cursor = cursor;
                        return true;
                    }
                    return false;
                }
                write_guard = RWLockWriteGuard(next->lock);
                cursor = next;
            }
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Aggregator::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainIterator>(this, std::move(snapshot));
    };

    class Aggregator::ChainRegexIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        Aggregator * _aggregator;
        AggregatorNode * _cursor;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _iter;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;

    public:
        ChainRegexIterator(const Aggregator * aggregator,
                           std::shared_ptr<Regex::R> regex,
                           std::unique_ptr<Snapshot> && snapshot)
                : _aggregator(const_cast<Aggregator *>(aggregator)),
                  _cursor(&_aggregator->_head),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)) { maySetNextValidCursorAndIter(); }
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
            maySetNextValidCursorAndIter();
        }

        std::pair<Slice, std::string>
        item() const override {
            assert(valid());
            return _iter->item();
        };

    private:
        void maySetNextValidCursorAndIter() {
            while (_iter == nullptr || !_iter->valid()) {
                _cursor = _cursor->next.get();
                if (_cursor != nullptr) {
                    RWLockReadGuard read_guard(_cursor->lock);
                    if (_cursor->db == nullptr) {
                        {
                            RWLockReadGuard _(std::move(read_guard));
                        }
                        RWLockWriteGuard write_guard(_cursor->lock);
                        _cursor->db = std::make_unique<DBSingle>(_cursor->db_name, Options{}, &_aggregator->_seq_gen);
                        _iter = _cursor->db->makeRegexIterator(_regex,
                                                               std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                    } else {
                        _iter = _cursor->db->makeRegexIterator(_regex,
                                                               std::make_unique<Snapshot>(_snapshot->immut_seq_num()));
                    }
                } else {
                    break;
                }
            }
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainRegexIterator>(this, std::move(regex), std::move(snapshot));
    };

    class Aggregator::ChainReversedRegexIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        Aggregator * _aggregator;
        AggregatorNode * _cursor;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _iter;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;

    public:
        ChainReversedRegexIterator(const Aggregator * aggregator,
                                   std::shared_ptr<Regex::R> regex,
                                   std::unique_ptr<Snapshot> && snapshot)
                : _aggregator(const_cast<Aggregator *>(aggregator)),
                  _cursor(&_aggregator->_head),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)) { maySetNextValidCursorAndIterBefore(nullptr); }
        DELETE_MOVE(ChainReversedRegexIterator);
        DELETE_COPY(ChainReversedRegexIterator);

    public:
        ~ChainReversedRegexIterator() noexcept override = default;

        bool valid() const override {
            return _iter != nullptr && _iter->valid();
        }

        void next() override {
            assert(valid());
            _iter->next();
            maySetNextValidCursorAndIterBefore(_cursor);
        }

        std::pair<Slice, std::string>
        item() const override {
            assert(valid());
            return _iter->item();
        };

    private:
        void maySetNextValidCursorAndIterBefore(AggregatorNode * node) {
            if (_iter != nullptr && _iter->valid()) {
                return;
            }

            AggregatorNode * cursor = &_aggregator->_head;
            AggregatorNode * next{};
            RWLockWriteGuard write_guard;
            while (true) {
                next = cursor->next.get();
                if (next == node) {
                    if (cursor != &_aggregator->_head) {
                        if (cursor->db == nullptr) {
                            cursor->db = std::make_unique<DBSingle>(cursor->db_name, Options{}, &_aggregator->_seq_gen);
                        }
                        _iter = cursor->db->makeRegexReversedIterator(_regex, std::make_unique<Snapshot>(
                                _snapshot->immut_seq_num()));
                        _cursor = cursor;
                        break;
                    }
                    return;
                }
                write_guard = RWLockWriteGuard(next->lock);
                cursor = next;
            }

            if (!_iter->valid()) {
                return maySetNextValidCursorAndIterBefore(_cursor);
            }
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                          std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ChainReversedRegexIterator>(this, std::move(regex), std::move(snapshot));
    };
}