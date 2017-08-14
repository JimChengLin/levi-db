#ifndef LEVIDB_ITERATOR_H
#define LEVIDB_ITERATOR_H

/*
 * 迭代器接口
 */

#include "slice.h"
#include "util.h"

namespace LeviDB {
    template<typename K, typename V>
    class Iterator {
    public:
        Iterator() noexcept = default;
        DEFAULT_MOVE(Iterator);
        DEFAULT_COPY(Iterator);

    public:
        virtual ~Iterator() noexcept = default;

        virtual bool valid() const = 0;

        virtual void seekToFirst() = 0;

        virtual void seekToLast() = 0;

        virtual void seek(const K & target) = 0;

        virtual void next() = 0;

        virtual void prev() = 0;

        virtual K key() const = 0;

        virtual V value() const = 0;
    };
}

#endif //LEVIDB_ITERATOR_H