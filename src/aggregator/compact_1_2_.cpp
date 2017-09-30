#ifndef __clang__
#include <algorithm>
#endif

#include "compact_1_2.h"

namespace LeviDB {
    class Compacting1To2IteratorMode1 : public Iterator<Slice, std::string> {
    private:
        const uint64_t _seq_num;
        const DB * _product_a;
        const DB * _product_b;
        const std::vector<Slice> _pending;
        std::vector<Slice>::const_iterator _pending_cursor{};
        std::unique_ptr<Iterator<Slice, std::string>> _resource_iter;

        enum Position {
            AT_PENDING,
            AT_RESOURCE,
            AT_NONE,
        };
        Position _current_at = AT_NONE;

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        Compacting1To2IteratorMode1(std::unique_ptr<Iterator<Slice, std::string>> && resource_iter,
                                    std::vector<Slice> pending,
                                    const DB * product_a,
                                    const DB * product_b,
                                    uint64_t seq_num) noexcept
                : _seq_num(seq_num),
                  _product_a(product_a),
                  _product_b(product_b),
                  _pending(std::move(pending)),
                  _resource_iter(std::move(resource_iter)) {}

        DELETE_MOVE(Compacting1To2IteratorMode1);
        DELETE_COPY(Compacting1To2IteratorMode1);

    public:
        ~Compacting1To2IteratorMode1() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void seekToFirst() override {
            _direction = FORWARD;
            _resource_iter->seekToFirst();
            _pending_cursor = _pending.cbegin();
            findSmallest();
        }

        void seekToLast() override {
            _direction = REVERSE;
            _resource_iter->seekToLast();
            _pending_cursor = _pending.cbegin() == _pending.cend() ? _pending.cend() : (--_pending.cend());
            findLargest();
        }

        void seek(const Slice & target) override {
            _direction = FORWARD;
            _resource_iter->seek(target);
            _pending_cursor = std::lower_bound(_pending.cbegin(), _pending.cend(), target, SliceComparator{});
            findSmallest();
        }

        void next() override {
            assert(valid());

            if (_direction != FORWARD) {
                _direction = FORWARD;
                if (_current_at == AT_PENDING) {
                    _resource_iter->seek(key());
                    if (_resource_iter->valid() && key() == _resource_iter->key()) {
                        _resource_iter->next();
                    }
                } else {
                    _pending_cursor = std::upper_bound(_pending.cbegin(), _pending.cend(), key(), SliceComparator{});
                }
            }

            if (_current_at == AT_PENDING) {
                ++_pending_cursor;
            } else {
                _resource_iter->next();
            }
            findSmallest();
        }

        void prev() override {
            assert(valid());

            if (_direction != REVERSE) {
                _direction = REVERSE;
                if (_current_at == AT_PENDING) {
                    _resource_iter->seek(key());
                    if (_resource_iter->valid()) {
                        _resource_iter->prev();
                    } else {
                        _resource_iter->seekToLast();
                    }
                } else {
                    _pending_cursor = std::lower_bound(_pending.cbegin(), _pending.cend(), key(), SliceComparator{});
                    if (_pending_cursor != _pending.cend()) {
                        if (_pending_cursor == _pending.cbegin()) {
                            _pending_cursor = _pending.cend();
                        } else {
                            --_pending_cursor;
                        }
                    } else if (_pending.cbegin() != _pending.cend()) {
                        --_pending_cursor;
                    }
                }
            }

            if (_current_at == AT_PENDING) {
                if (_pending_cursor == _pending.cbegin()) {
                    _pending_cursor = _pending.cend();
                } else {
                    --_pending_cursor;
                }
            } else {
                _resource_iter->prev();
            }
            findLargest();
        }

        Slice key() const override {
            assert(valid());
            if (_current_at == AT_PENDING) {
                return *_pending_cursor;
            }
            return _resource_iter->key();
        }

        std::string value() const override {
            assert(valid());
            switch (_current_at) {
                case AT_PENDING: {
                    ReadOptions options;
                    options.sequence_number = _seq_num;
                    auto a_res = _product_a->get(options, *_pending_cursor);
                    if (a_res.second) {
                        return a_res.first;
                    }
                    auto b_res = _product_b->get(options, *_pending_cursor);
                    assert(b_res.second);
                    return b_res.first;
                }

                case AT_RESOURCE:
                    return _resource_iter->value();

                case AT_NONE:; // impossible if valid
            }
            return {};
        }

    private:
        void findSmallest() {
            if (_pending_cursor == _pending.cend() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }

            if (_pending_cursor != _pending.cend() && _resource_iter->valid()
                && _resource_iter->key() == *_pending_cursor) {
                _resource_iter->next();
                _current_at = AT_PENDING;
            }

            if (_pending_cursor != _pending.cend()) {
                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, *_pending_cursor);
                if (a_res.second) {
                } else {
                    auto b_res = _product_b->get(options, *_pending_cursor);
                    if (b_res.second) {
                    } else {
                        ++_pending_cursor;
                        return findSmallest();
                    }
                }
            }

            if (_pending_cursor == _pending.cend()) {
                _current_at = AT_RESOURCE;
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_PENDING;
                return;
            }

            if (SliceComparator{}(_resource_iter->key(), *_pending_cursor)) {
                _current_at = AT_RESOURCE;
            } else {
                assert(_resource_iter->key() != *_pending_cursor);
                _current_at = AT_PENDING;
            }
        }

        void findLargest() {
            if (_pending_cursor == _pending.cend() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }

            if (_pending_cursor != _pending.cend() && _resource_iter->valid()
                && _resource_iter->key() == *_pending_cursor) {
                _resource_iter->prev();
                _current_at = AT_PENDING;
            }

            if (_pending_cursor != _pending.cend()) {
                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, *_pending_cursor);
                if (a_res.second) {
                } else {
                    auto b_res = _product_b->get(options, *_pending_cursor);
                    if (b_res.second) {
                    } else {
                        if (_pending_cursor == _pending.cbegin()) {
                            _pending_cursor = _pending.cend();
                        } else {
                            --_pending_cursor;
                        }
                        return findLargest();
                    }
                }
            }

            if (_pending_cursor == _pending.cend()) {
                _current_at = AT_RESOURCE;
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_PENDING;
                return;
            }

            if (SliceComparator{}(*_pending_cursor, _resource_iter->key())) {
                _current_at = AT_RESOURCE;
            } else {
                assert(_resource_iter->key() != *_pending_cursor);
                _current_at = AT_PENDING;
            }
        }
    };

    class Compacting1To2IteratorMode2 : public Iterator<Slice, std::string> {
    private:
        std::unique_ptr<Iterator<Slice, std::string>> _a_iter;
        std::unique_ptr<Iterator<Slice, std::string>> _b_iter;

        enum Position {
            AT_A,
            AT_B,
            AT_NONE,
        };
        Position _current_at = AT_NONE;

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        Compacting1To2IteratorMode2(std::unique_ptr<Iterator<Slice, std::string>> && a_iter,
                                    std::unique_ptr<Iterator<Slice, std::string>> && b_iter) noexcept
                : _a_iter(std::move(a_iter)), _b_iter(std::move(b_iter)) {}

        DELETE_MOVE(Compacting1To2IteratorMode2);
        DELETE_COPY(Compacting1To2IteratorMode2);

    public:
        ~Compacting1To2IteratorMode2() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void seekToFirst() override {
            _direction = FORWARD;
            _current_at = AT_A;
            _a_iter->seekToFirst();
            _b_iter->seekToFirst();
        }

        void seekToLast() override {
            _direction = REVERSE;
            _current_at = AT_B;
            _a_iter->seekToLast();
            _b_iter->seekToLast();
        }

        void seek(const Slice & target) override {
            _direction = FORWARD;
            _a_iter->seek(target);
            if (_a_iter->valid()) {
                _current_at = AT_A;
                _b_iter->seekToFirst();
            } else {
                _b_iter->seek(target);
                _current_at = _b_iter->valid() ? AT_B : AT_NONE;
            }
        }

        void next() override {
            assert(valid());

            if (_direction != FORWARD) {
                _direction = FORWARD;
                if (_current_at == AT_A) {
                    _b_iter->seekToFirst();
                }
            }

            if (_current_at == AT_A) {
                _a_iter->next();
                if (_a_iter->valid()) {
                } else {
                    _current_at = AT_B;
                }
            } else {
                _b_iter->next();
                if (_b_iter->valid()) {
                } else {
                    _current_at = AT_NONE;
                }
            }
        }

        void prev() override {
            assert(valid());

            if (_direction != REVERSE) {
                _direction = REVERSE;
                if (_current_at == AT_A) {
                } else {
                    _a_iter->seekToLast();
                }
            }

            if (_current_at == AT_A) {
                _a_iter->prev();
                if (_a_iter->valid()) {
                } else {
                    _current_at = AT_NONE;
                }
            } else {
                _b_iter->prev();
                if (_b_iter->valid()) {
                } else {
                    _current_at = AT_A;
                }
            }
        }

        Slice key() const override {
            assert(valid());
            if (_current_at == AT_A) {
                return _a_iter->key();
            }
            return _b_iter->key();
        }

        std::string value() const override {
            assert(valid());
            if (_current_at == AT_A) {
                return _a_iter->value();
            }
            return _b_iter->value();
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Compacting1To2DB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        uint64_t seq_num = snapshot->immut_seq_num();
        if (seq_num < _action_num) {
            return _resource->makeIterator(std::move(snapshot));
        }

        RWLockReadGuard read_guard(_rwlock);
        if (_compacting) {
            return std::make_unique<Compacting1To2IteratorMode1>(_resource->makeIterator(std::move(snapshot)),
                                                                 pendingPartUnlocked(seq_num),
                                                                 _product_a.get(),
                                                                 _product_b.get(),
                                                                 seq_num);
        }

        return std::make_unique<Compacting1To2IteratorMode2>(
                _product_a->makeIterator(std::make_unique<Snapshot>(seq_num)),
                _product_b->makeIterator(std::move(snapshot)));
    };

    template<bool REVERSE>
    class Compacting1To2RegexIteratorMode1 : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        const uint64_t _seq_num;
        const DB * _product_a;
        const DB * _product_b;
        const std::vector<Slice> _pending;
        std::vector<Slice>::const_iterator _pending_cursor;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _resource_iter;
        std::shared_ptr<Regex::R> _regex;

        enum Position {
            AT_PENDING,
            AT_RESOURCE,
            AT_NONE,
        };
        Position _current_at = AT_NONE;

    public:
        Compacting1To2RegexIteratorMode1(
                std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> && resource_iter,
                std::vector<Slice> pending,
                std::shared_ptr<Regex::R> regex,
                const DB * product_a,
                const DB * product_b,
                uint64_t seq_num)
                : _seq_num(seq_num),
                  _product_a(product_a),
                  _product_b(product_b),
                  _pending(std::move(pending)),
                  _pending_cursor(_pending.cbegin()),
                  _resource_iter(std::move(resource_iter)),
                  _regex(std::move(regex)) { findSmallest(); }

        DELETE_MOVE(Compacting1To2RegexIteratorMode1);
        DELETE_COPY(Compacting1To2RegexIteratorMode1);

    public:
        ~Compacting1To2RegexIteratorMode1() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void next() override {
            assert(valid());
            if (_current_at == AT_PENDING) {
                ++_pending_cursor;
            } else {
                _resource_iter->next();
            }
            findSmallest();
        }

        std::pair<Slice, std::string> item() const override {
            assert(valid());
            switch (_current_at) {
                case AT_PENDING: {
                    ReadOptions options;
                    options.sequence_number = _seq_num;
                    auto a_res = _product_a->get(options, *_pending_cursor);
                    if (a_res.second) {
                        return {*_pending_cursor, a_res.first};
                    }
                    auto b_res = _product_b->get(options, *_pending_cursor);
                    assert(b_res.second);
                    return {*_pending_cursor, b_res.first};
                }

                case AT_RESOURCE:
                    return _resource_iter->item();

                case AT_NONE:;
            }
            return {};
        };

    private:
        void findSmallest() {
            if (_pending_cursor == _pending.cend() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }

            if (_pending_cursor != _pending.cend() && _resource_iter->valid()
                                                      && _resource_iter->item().first == *_pending_cursor) {
                _resource_iter->next();
                _current_at = AT_PENDING;
            }

            if (_pending_cursor != _pending.cend()) {
                if (!_regex->match(_pending_cursor->toString())) {
                    ++_pending_cursor;
                    return findSmallest();
                }

                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, *_pending_cursor);
                if (a_res.second) {
                } else {
                    auto b_res = _product_b->get(options, *_pending_cursor);
                    if (b_res.second) {
                    } else {
                        ++_pending_cursor;
                        return findSmallest();
                    }
                }
            }

            if (_pending_cursor == _pending.cend()) {
                _current_at = AT_RESOURCE;
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_PENDING;
                return;
            }

            if (SliceComparator{}(_resource_iter->item().first, *_pending_cursor)) {
                _current_at = !REVERSE ? AT_RESOURCE : AT_PENDING;
            } else {
                assert(_resource_iter->item().first != *_pending_cursor);
                _current_at = !REVERSE ? AT_PENDING : AT_RESOURCE;
            }
        }
    };

    class Compacting1To2RegexIteratorMode2 : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _a_iter;
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> _b_iter;

        enum Position {
            AT_A,
            AT_B,
            AT_NONE,
        };
        Position _current_at = AT_A;

    public:
        Compacting1To2RegexIteratorMode2(std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> && a_iter,
                                         std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>> && b_iter)
        noexcept : _a_iter(std::move(a_iter)), _b_iter(std::move(b_iter)) {}

        DELETE_MOVE(Compacting1To2RegexIteratorMode2);
        DELETE_COPY(Compacting1To2RegexIteratorMode2);

    public:
        ~Compacting1To2RegexIteratorMode2() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void next() override {
            assert(valid());
            if (_current_at == AT_A) {
                _a_iter->next();
                if (_a_iter->valid()) {
                } else {
                    _current_at = AT_B;
                }
            } else {
                _b_iter->next();
                if (_b_iter->valid()) {
                } else {
                    _current_at = AT_NONE;
                }
            }
        }

        std::pair<Slice, std::string> item() const override {
            assert(valid());
            if (_current_at == AT_A) {
                return _a_iter->item();
            }
            return _b_iter->item();
        };
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        uint64_t seq_num = snapshot->immut_seq_num();
        if (seq_num < _action_num) {
            return _resource->makeRegexIterator(std::move(regex), std::move(snapshot));
        }

        RWLockReadGuard read_guard(_rwlock);
        if (_compacting) {
            return std::make_unique<Compacting1To2RegexIteratorMode1<false>>(
                    _resource->makeRegexIterator(regex, std::move(snapshot)),
                    pendingPartUnlocked(seq_num),
                    regex,
                    _product_a.get(),
                    _product_b.get(),
                    seq_num);
        }

        return std::make_unique<Compacting1To2RegexIteratorMode2>(
                _product_a->makeRegexIterator(regex, std::make_unique<Snapshot>(seq_num)),
                _product_b->makeRegexIterator(regex, std::move(snapshot)));
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                                std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        uint64_t seq_num = snapshot->immut_seq_num();
        if (seq_num < _action_num) {
            return _resource->makeRegexReversedIterator(std::move(regex), std::move(snapshot));
        }

        RWLockReadGuard read_guard(_rwlock);
        if (_compacting) {
            std::vector<Slice> pending = pendingPartUnlocked(seq_num);
            std::reverse(pending.begin(), pending.end());
            return std::make_unique<Compacting1To2RegexIteratorMode1<true>>(
                    _resource->makeRegexReversedIterator(regex, std::move(snapshot)),
                    std::move(pending),
                    regex,
                    _product_a.get(),
                    _product_b.get(),
                    seq_num);
        }

        return std::make_unique<Compacting1To2RegexIteratorMode2>(
                _product_b->makeRegexReversedIterator(regex, std::make_unique<Snapshot>(seq_num)),
                _product_a->makeRegexReversedIterator(regex, std::move(snapshot)));
    };
}