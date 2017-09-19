#ifndef LEVIDB_MERGER_H
#define LEVIDB_MERGER_H

/*
 * 将多个迭代器合并成一个
 *
 * steal from leveldb
 */

#include <cassert>
#include <functional>
#include <memory>
#include <vector>

#include "iterator.h"
#include "optional.h"
#include "util.h"

namespace LeviDB {
    template<typename K, typename V, typename CMP = std::less<K>>
    class MergingIterator : public Iterator<K, V> {
    private:
        using inner_iter_t = Iterator<K, V>;

        inner_iter_t * _current = nullptr;
        std::vector<std::unique_ptr<inner_iter_t>> _children;
        CMP _cmp{};

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        MergingIterator() noexcept = default;

        DELETE_MOVE(MergingIterator);
        DELETE_COPY(MergingIterator);

        void addIterator(std::unique_ptr<inner_iter_t> && iter) noexcept {
            _children.emplace_back(std::move(iter));
        }

        bool doForward() const noexcept {
            return _direction == FORWARD;
        }

        bool doBackward() const noexcept {
            return _direction == REVERSE;
        }

    public:
        ~MergingIterator() noexcept override = default;

        bool valid() const override {
            return _current != nullptr;
        };

        void seekToFirst() override {
            for (auto & child : _children) {
                child->seekToFirst();
            }
            findSmallest();
            _direction = FORWARD;
        };

        void seekToLast() override {
            for (auto & child : _children) {
                child->seekToLast();
            }
            findLargest();
            _direction = REVERSE;
        };

        void seek(const K & target) override {
            for (auto & child : _children) {
                child->seek(target);
            }
            findSmallest();
            _direction = FORWARD;
        };

        void next() override {
            assert(valid());

            if (_direction != FORWARD) {
                for (auto & child : _children) {
                    if (child.get() != _current) {
                        child->seek(key());
                        // ensure that all children are positioned after key()
                        if (child->valid() && key() == child->key()) {
                            child->next();
                        }
                    }
                }
                _direction = FORWARD;
            }

            _current->next();
            findSmallest();
        };

        void prev() override {
            assert(valid());

            if (_direction != REVERSE) {
                for (auto & child : _children) {
                    if (child.get() != _current) {
                        child->seek(key());
                        if (child->valid()) {
                            // entry >= key(), step back one
                            child->prev();
                        } else {
                            child->seekToLast();
                        }
                    }
                }
                _direction = REVERSE;
            }

            _current->prev();
            findLargest();
        };

        K key() const override {
            return _current->key();
        };

        V value() const override {
            return _current->value();
        };

    private:
        void findSmallest() {
            inner_iter_t * smallest = nullptr;
            for (const auto & child : _children) {
                if (child->valid()) {
                    if (smallest == nullptr || _cmp(child->key(), smallest->key())) {
                        smallest = child.get();
                    }
                }
            }
            _current = smallest;
        }

        void findLargest() {
            inner_iter_t * largest = nullptr;
            for (const auto & child : _children) {
                if (child->valid()) {
                    if (largest == nullptr || _cmp(largest->key(), child->key())) {
                        largest = child.get();
                    }
                }
            }
            _current = largest;
        }
    };

    template<typename T, bool SMALL_FIRST = true, typename CMP = std::less<T>>
    class MergingSimpleIterator final : public SimpleIterator<T> {
    private:
        using inner_iter_t = SimpleIterator<T>;

        std::vector<std::unique_ptr<inner_iter_t>> _children;
        std::vector<Optional<T>> _cache;
        size_t cursor_at = 0;
        CMP _cmp{};

    public:
        explicit MergingSimpleIterator(std::vector<std::unique_ptr<inner_iter_t>> && children)
                : _children(children), _cache(_children.size()) {
            for (int i = 0; i < _children.size(); ++i) {
                if (_children[i]->valid()) {
                    _cache[i].build(_children[i]->item());
                }
            }

            if (SMALL_FIRST) {
                findSmallest();
            } else {
                findLargest();
            }
        };

        DELETE_MOVE(MergingSimpleIterator);
        DELETE_COPY(MergingSimpleIterator);

    public:
        ~MergingSimpleIterator() noexcept override = default;

        bool valid() const override {
            return _cache[cursor_at].valid();
        }

        void next() override {
            _cache[cursor_at].reset();
            _children[cursor_at]->next();
            if (_children[cursor_at]->valid()) {
                _cache[cursor_at].build(_children[cursor_at]->item());
            }

            if (SMALL_FIRST) {
                findSmallest();
            } else {
                findLargest();
            }
        }

        T item() const override {
            return _cache[cursor_at];
        }

    private:
        void findSmallest() {
            static_assert(SMALL_FIRST, "wrong direction");
            size_t smallest_at = 0;
            for (size_t i = 1; i < _cache.size(); ++i) {
                if (_cache[i].valid() &&
                    (!_cache[smallest_at].valid() || _cmp(*_cache[i].get(), *_cache[smallest_at].get()))) {
                    smallest_at = i;
                }
            }
            cursor_at = smallest_at;
        }

        void findLargest() {
            static_assert(!SMALL_FIRST, "wrong direction");
            size_t largest_at = 0;
            for (size_t i = 1; i < _cache.size(); ++i) {
                if (_cache[i].valid() &&
                    (!_cache[largest_at].valid() || _cmp(*_cache[largest_at].get()), *_cache[i].get())) {
                    largest_at = i;
                }
            }
            cursor_at = largest_at;
        }
    };
}

#endif //LEVIDB_MERGER_H