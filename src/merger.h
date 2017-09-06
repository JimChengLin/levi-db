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
#include "util.h"

namespace LeviDB {
    template<typename K, typename V, typename CMP=std::less<K>>
    class MergingIterator : public Iterator<K, V> {
    private:
        Iterator<K, V> * _current = nullptr;
        std::vector<std::unique_ptr<Iterator<K, V>>> _children;
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

        void addIterator(std::unique_ptr<Iterator<K, V>> && iter) noexcept {
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
            Iterator<K, V> * smallest = nullptr;
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
            Iterator<K, V> * largest = nullptr;
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
}

#endif //LEVIDB_MERGER_H