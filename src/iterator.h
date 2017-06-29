#ifndef LEVIDB_ITERATOR_H
#define LEVIDB_ITERATOR_H

/*
 * 自定义迭代器
 */

#include "slice.h"

namespace LeviDB {
    class Iterator {
    public:
        Iterator() noexcept {};

        virtual ~Iterator() noexcept {};

        virtual bool valid() const = 0;

        virtual void seekToFirst() = 0;

        virtual void seekToLast() = 0;

        virtual void seek(const Slice & target) = 0;

        virtual void next() = 0;

        virtual void prev() = 0;

        virtual Slice key() const = 0;

        virtual Slice value() const = 0;

    private:
        // 禁止复制
        Iterator(const Iterator &);

        void operator=(const Iterator &);
    };

    // 缓存结果的迭代器
    class IteratorWrapper {
    public:
        IteratorWrapper() noexcept : _iter(nullptr), _valid(false) {}

        explicit IteratorWrapper(Iterator * iter) {
            set(iter);
        }

        ~IteratorWrapper() noexcept {}

        Iterator * iter() const noexcept { return _iter.get(); }

        // 获得所有权
        void set(Iterator * iter) {
            _iter = iter;
            if (_iter == nullptr) {
                _valid = false;
            } else {
                update();
            }
        }

        bool valid() const noexcept { return _valid; }

        Slice key() const noexcept {
            assert(valid());
            return _key;
        }

        Slice value() const noexcept {
            assert(valid());
            return _value;
        }

        void next() {
            assert(_iter);
            _iter->next();
            update();
        }

        void prev() {
            assert(_iter);
            _iter->prev();
            update();
        }

        void seek(const Slice & k) {
            assert(_iter);
            _iter->seek(k);
            update();
        }

        void seekToFirst() {
            assert(_iter);
            _iter->seekToFirst();
            update();
        }

        void seekToLast() {
            assert(_iter);
            _iter->seekToLast();
            update();
        }

    private:
        void update() {
            _valid = _iter->valid();
            if (_valid) {
                _key = _iter->key();
                _value = _iter->value();
            }
        }

        std::unique_ptr<Iterator> _iter;
        Slice _key;
        Slice _value;
        bool _valid;
    };
}

#endif //LEVIDB_ITERATOR_H