#include "compact_1_2.h"
#include <iostream>

namespace LeviDB {
    class Compacting1To2Iterator : public Iterator<Slice, std::string> {
    private:
        // mode 1
        const uint64_t _seq_num;
        const DB * _product_a;
        const DB * _product_b;
        const std::atomic<bool> * _compacting;
        const std::set<std::string, SliceComparator> * _ignore;
        const ReadWriteLock * _ignore_lock;
        std::unique_ptr<Iterator<Slice, std::string>> _resource_iter;

        // mode 2
        std::unique_ptr<Iterator<Slice, std::string>> _a_iter;
        std::unique_ptr<Iterator<Slice, std::string>> _b_iter;

        std::string _key_;
        Slice _key;

        enum Position {
            AT_IGNORE,
            AT_RESOURCE,
            AT_A,
            AT_B,
            AT_NONE,
        };
        mutable Position _current_at = AT_NONE;

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
                  _ignore_lock(ignore_lock),
                  _resource_iter(std::move(resource_iter)) {

//            for (auto begin = (*_ignore).cbegin(); begin != (*_ignore).cend(); ++begin) {
//                printf("ignore %s\n", (*begin).c_str());
//            }
        }

        DELETE_MOVE(Compacting1To2Iterator);
        DELETE_COPY(Compacting1To2Iterator);

    public:
        ~Compacting1To2Iterator() noexcept override = default;

        bool valid() const override {
            return _current_at != AT_NONE;
        }

        void seekToFirst() override {
            _direction = FORWARD;

            if (true) {
                _mode = MODE_1;

                _resource_iter->seekToFirst();
                RWLockReadGuard read_guard(*_ignore_lock);
                if (true) {
                    _key_ = _ignore->cbegin() != _ignore->cend() ? *_ignore->cbegin() : "";
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
                RWLockReadGuard read_guard(*_ignore_lock);
                if (*_compacting) {
                    _key_ = _ignore->cbegin() != _ignore->cend() ? *(--_ignore->cend()) : "";
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
                RWLockReadGuard read_guard(*_ignore_lock);
                if (*_compacting) {
                    auto res = _ignore->lower_bound(target);
                    _key_ = res != _ignore->cend() ? *res : "";
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
            assert(_mode == MODE_1);
//            if (!*_compacting) {
//                seek(_key);
//                return next();
//            }
            RWLockReadGuard read_guard(*_ignore_lock);
//            if (!*_compacting) {
//                seek(_key);
//                return next();
//            }

            if (_current_at == AT_IGNORE) {
                if (!_key_.empty()) {
                    auto res = _ignore->upper_bound(_key_);
                    _key_ = res != _ignore->cend() ? *res : "";
                }
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
            RWLockReadGuard read_guard(*_ignore_lock);
            if (!*_compacting) {
                seek(_key);
                return prev();
            }

            if (_current_at == AT_IGNORE) {
                if (!_key_.empty()) {
                    auto res = _ignore->lower_bound(_key_);
                    if (res != _ignore->cbegin()) {
                        --res;
                    } else {
                        res = _ignore->cend();
                    }
                    _key_ = res != _ignore->cend() ? *res : "";
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
                case AT_IGNORE: {
//                    printf("key %s at Ignore", key().toString().c_str());
                    ReadOptions options;
                    options.sequence_number = _seq_num;
                    auto a_res = _product_a->get(options, _key_);
                    if (a_res.second) {
                        return a_res.first;
                    } else {
                        auto b_res = _product_b->get(options, _key_);
                        assert(b_res.second);
                        return b_res.first;
                    }
                }

                case AT_RESOURCE:
//                    if (_ignore->find(key()) == _ignore->cend()) {
//                        _current_at = AT_IGNORE;
//                        return value();
//                    }
//                    assert(_ignore->find(key()) == _ignore->cend());

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
            if (_key_.empty() && !_resource_iter->valid()) {
                _current_at = AT_NONE;
                return;
            }
            if (!_key_.empty()) {
                ReadOptions options{};
                options.sequence_number = _seq_num;
                auto a_res = _product_a->get(options, _key_);
                if (a_res.second) {
                } else {
                    auto b_res = _product_b->get(options, _key_);
                    if (b_res.second) {
                    } else {
//                        printf("cannot find %s\n", _key_.c_str());
                        auto res = _ignore->upper_bound(_key_);
                        if (res != _ignore->cend()) {
                            _key_ = *res;
                        } else {
                            _key_.clear();
                        }
                        return findSmallest();
                    }
                }
            }
            if (_key_.empty()) {
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
                return;
            }
            if (!_resource_iter->valid()) {
                _current_at = AT_IGNORE;
                _key = _key_;
                return;
            }
            if (_resource_iter->key() == _key_) {
                _resource_iter->next();
                _current_at = AT_IGNORE;
                _key = _key_;
                return;
            }

            if (SliceComparator{}(_resource_iter->key(), _key_)) {
                _current_at = AT_RESOURCE;
                _key = _resource_iter->key();
            } else {
                assert(_resource_iter->key() != _key_);
                _current_at = AT_IGNORE;
                _key = _key_;
            }
        }

        void findLargest() {
        }

        void switchToMode2() {
            assert(false);
//            assert(!*_compacting);
//            _mode = MODE_2;
//            if (_a_iter == nullptr) {
//                assert(_b_iter == nullptr);
//                _a_iter = _product_a->makeIterator(std::make_unique<Snapshot>(_seq_num));
//                _b_iter = _product_b->makeIterator(std::make_unique<Snapshot>(_seq_num));
//            }
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Compacting1To2DB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        RWLockReadGuard a(_a_lock);
        RWLockReadGuard b(_b_lock);
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