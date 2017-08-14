#ifndef LEVIDB_ITERATOR_H
#define LEVIDB_ITERATOR_H

/*
 * 迭代器接口
 */

#include "slice.h"
#include "util.h"

namespace LeviDB {
    class Iterator {
    public:
        Iterator() noexcept = default;
        DELETE_MOVE(Iterator);
        DELETE_COPY(Iterator);

    public:
        virtual ~Iterator() noexcept = default;

        virtual bool valid() const = 0;

        virtual void seekToFirst() = 0;

        virtual void seekToLast() = 0;

        virtual void seek(const Slice & target) = 0;

        virtual void next() = 0;

        virtual void prev() = 0;

        virtual Slice key() const = 0;

        virtual Slice value() const = 0;
    };
}

#endif //LEVIDB_ITERATOR_H