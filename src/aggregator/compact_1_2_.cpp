#include "compact_1_2.h"

namespace LeviDB {
    class Compacting1To2Iterator : public Iterator<Slice, std::string> {
    private:
        // mode 1
        const uint64_t _seq_num;
        const DB * _product_a;
        const DB * _product_b;
        const std::atomic<bool> * _compacting;
        const std::set<std::string, SliceComparator> * _ignore;
        const ReadWriteLock * _rd_only_lock;
        std::set<std::string, SliceComparator>::const_iterator _ignore_cursor;
        std::unique_ptr<Iterator<Slice, std::string>> _resource_iter;

        // mode 2
        std::unique_ptr<Iterator<Slice, std::string>> _a_iter;
        std::unique_ptr<Iterator<Slice, std::string>> _b_iter;

        std::string _key_;
        std::string _value_;
        Slice _key;

        enum Position {
            AT_IGNORE,
            AT_RESOURCE,
            AT_A,
            AT_B,
            AT_NONE,
        };
        Position _current_at = AT_NONE;

        enum IterMode {
            MODE_1,
            MODE_2,
            MODE_NONE,
        };
        IterMode _mode = MODE_NONE;

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        Compacting1To2Iterator(std::unique_ptr<Iterator<Slice, std::string>> && resource_iter,
                               const std::set<std::string, SliceComparator> * ignore,
                               const ReadWriteLock * ignore_lock,
                               const DB * product_a,
                               const DB * product_b,
                               const std::atomic<bool> * compacting,
                               uint64_t seq_num) noexcept
                : _seq_num(seq_num),
                  _product_a(product_a),
                  _product_b(product_b),
                  _compacting(compacting),
                  _ignore(ignore),
                  _rd_only_lock(ignore_lock),
                  _resource_iter(std::move(resource_iter)) {}

        DELETE_MOVE(Compacting1To2Iterator);
        DELETE_COPY(Compacting1To2Iterator);

    public:
        ~Compacting1To2Iterator() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void seekToFirst() override {
            _direction = FORWARD;

            if (*_compacting) {
                _mode = MODE_1;

                _resource_iter->seekToFirst();
                RWLockReadGuard read_guard(*_rd_only_lock);
                if (*_compacting) {
                    _ignore_cursor = _ignore->cbegin();
                    findSmallest();
                    return;
                }
            }

            switchToMode2();
            _a_iter->seekToFirst();
            _b_iter->seekToFirst();
            _current_at = AT_A;
            _key = _a_iter->key();
        }

        void seekToLast() override {
            _direction = REVERSE;

            if (*_compacting) {
                _mode = MODE_1;

                _resource_iter->seekToLast();
                RWLockReadGuard read_guard(*_rd_only_lock);
                if (*_compacting) {
                    _ignore_cursor = (_ignore->cbegin() == _ignore->cend() ? _ignore->cend() : --_ignore->cend());
                    findLargest();
                    return;
                }
            }

            switchToMode2();
            _a_iter->seekToLast();
            _b_iter->seekToLast();
            _current_at = AT_B;
            _key = _b_iter->key();
        }

        void seek(const Slice & target) override {
            _direction = FORWARD;

            if (*_compacting) {
                _mode = MODE_1;

                _resource_iter->seek(target);
                RWLockReadGuard read_guard(*_rd_only_lock);
                if (*_compacting) {
                    _ignore_cursor = _ignore->lower_bound(target);
                    findSmallest();
                    return;
                }
            }

            switchToMode2();
            _a_iter->seek(target);
            if (_a_iter->valid()) {
                _current_at = AT_A;
                _key = _a_iter->key();
                _b_iter->seekToFirst();
            } else {
                _b_iter->seek(target);
                if (_b_iter->valid()) {
                    _current_at = AT_B;
                    _key = _b_iter->key();
                } else {
                    _current_at = AT_NONE;
                }
            }
        }

        void next() override {
            assert(valid());

            if (_mode == MODE_2) {
                if (_direction != FORWARD) {
                    if (_current_at == AT_A) {
                        _b_iter->seekToFirst();
                    } else {
                        assert(_current_at == AT_B);
                    }
                    _direction = FORWARD;
                }

                if (_current_at == AT_A) {
                    _a_iter->next();
                    if (_a_iter->valid()) {
                        _key = _a_iter->key();
                    } else {
                        if (_b_iter->valid()) {
                            _current_at = AT_B;
                            _key = _b_iter->key();
                        } else {
                            _current_at = AT_NONE;
                        }
                    }
                } else {
                    assert(_current_at == AT_B);
                    _b_iter->next();
                    if (_b_iter->valid()) {
                        _key = _b_iter->key();
                    } else {
                        _current_at = AT_NONE;
                    }
                }
                return;
            }

            assert(_mode == MODE_1);
            if (!*_compacting) {
                seek(_key);
                return next();
            }
            RWLockReadGuard read_guard(*_rd_only_lock);
            if (!*_compacting) {
                seek(_key);
                return next();
            }

            if (_current_at == AT_IGNORE) {
                ++_ignore_cursor;
            } else {
                assert(_current_at == AT_RESOURCE);
                _resource_iter->next();
            }
            findSmallest();
        }

        void prev() override {
            assert(valid());

            if (_mode == MODE_2) {
                if (_direction != REVERSE) {
                    if (_current_at == AT_A) {
                    } else {
                        assert(_current_at == AT_B);
                        _a_iter->seekToLast();
                    }
                    _direction = REVERSE;
                }

                if (_current_at == AT_A) {
                    _a_iter->prev();
                    if (_a_iter->valid()) {
                        _key = _a_iter->key();
                    } else {
                        _current_at = AT_NONE;
                    }
                } else {
                    assert(_current_at == AT_B);
                    _b_iter->prev();
                    if (_b_iter->valid()) {
                        _key = _b_iter->key();
                    } else {
                        if (_a_iter->valid()) {
                            _current_at = AT_A;
                            _key = _a_iter->key();
                        } else {
                            _current_at = AT_NONE;
                        }
                    }
                }
            }

            assert(_mode == MODE_1);
            if (!*_compacting) {
                seek(_key);
                return prev();
            }
            RWLockReadGuard read_guard(*_rd_only_lock);
            if (!*_compacting) {
                seek(_key);
                return prev();
            }

            if (_current_at == AT_IGNORE) {
                if (_ignore_cursor == _ignore->cbegin()) {
                    _ignore_cursor = _ignore->cend();
                } else {
                    --_ignore_cursor;
                }
            } else {
                assert(_current_at == AT_RESOURCE);
                _resource_iter->prev();
            }
            findLargest();
        }

        Slice key() const override {
            assert(valid());
            return _key;
        }

        std::string value() const override {
            assert(valid());
            switch (_current_at) {
                case AT_IGNORE:
                    return _value_;

                case AT_RESOURCE:
                    return _resource_iter->value();

                case AT_A:
                    return _a_iter->value();

                case AT_B:
                    return _b_iter->value();

                case AT_NONE:; // impossible if valid
            }
            return {};
        }

    private:
        void findSmallest() {
            assert(_mode == MODE_1);
            if (_ignore_cursor == _ignore->cbegin() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }
            if (_ignore_cursor != _ignore->cbegin()) {
                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, *_ignore_cursor);
                if (a_res.second) {
                    _value_ = std::move(a_res.first);
                } else {
                    auto b_res = _product_b->get(options, *_ignore_cursor);
                    if (b_res.second) {
                        _value_ = std::move(b_res.first);
                    } else {
                        ++_ignore_cursor;
                        return findSmallest();
                    }
                }
            }
            if (_ignore_cursor == _ignore->cbegin()) {
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_IGNORE;
                _key_ = *_ignore_cursor;
                _key = _key_;
                return;
            }
            if (_resource_iter->key() == *_ignore_cursor) {
                _resource_iter->next();
                return findSmallest();
            }

            if (SliceComparator{}(_resource_iter->key(), *_ignore_cursor)) {
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
            } else {
                assert(_resource_iter->key() != *_ignore_cursor);
                _current_at = AT_IGNORE;
                _key_ = *_ignore_cursor;
                _key = _key_;
            }
        }

        void findLargest() {
            assert(_mode == MODE_1);
            if (_ignore_cursor == _ignore->cbegin() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }
            if (_ignore_cursor != _ignore->cbegin()) {
                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, *_ignore_cursor);
                if (a_res.second) {
                    _value_ = std::move(a_res.first);
                } else {
                    auto b_res = _product_b->get(options, *_ignore_cursor);
                    if (b_res.second) {
                        _value_ = std::move(b_res.first);
                    } else {
                        if (_ignore_cursor == _ignore->cbegin()) {
                            _ignore_cursor = _ignore->cend();
                        } else {
                            --_ignore_cursor;
                        }
                        return findLargest();
                    }
                }
            }
            if (_ignore_cursor == _ignore->cbegin()) {
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_IGNORE;
                _key_ = *_ignore_cursor;
                _key = _key_;
                return;
            }
            if (_resource_iter->key() == *_ignore_cursor) {
                _resource_iter->prev();
                return findLargest();
            }

            if (SliceComparator{}(_resource_iter->key(), *_ignore_cursor)) {
                _current_at = AT_IGNORE;
                _key_ = *_ignore_cursor;
                _key = _key_;
            } else {
                assert(_resource_iter->key() != *_ignore_cursor);
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
            }
        }

        void switchToMode2() {
            assert(!*_compacting);
            _mode = MODE_2;
            if (_a_iter == nullptr) {
                assert(_b_iter == nullptr);
                _a_iter = _product_a->makeIterator(std::make_unique<Snapshot>(_seq_num));
                _b_iter = _product_b->makeIterator(std::make_unique<Snapshot>(_seq_num));
            }
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Compacting1To2DB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        uint64_t seq_num = snapshot->immut_seq_num();
        return std::make_unique<Compacting1To2Iterator>(_resource->makeIterator(std::move(snapshot)),
                                                        &_ignore,
                                                        &_rw_lock,
                                                        _product_a.get(),
                                                        _product_b.get(),
                                                        &_compacting,
                                                        seq_num);
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                                std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
    };
}