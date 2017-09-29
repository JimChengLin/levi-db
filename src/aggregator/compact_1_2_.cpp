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
        }

        void seek(const Slice & target) override {
            _direction = FORWARD;
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

        void prev() override {
            assert(valid());
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
                assert(_current_at == AT_B);
                _b_iter->next();
                if (_b_iter->valid()) {
                } else {
                    _current_at = AT_NONE;
                }
            }
        }

        void prev() override {
            assert(valid());
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