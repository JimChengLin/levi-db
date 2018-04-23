#pragma once
#ifndef LEVIDB_ITERATOR_H
#define LEVIDB_ITERATOR_H

/*
 * 迭代器接口
 */

#include "slice.h"

namespace levidb {
    class Iterator {
    public:
        Iterator() = default;

        virtual ~Iterator() = default;

    public:
        virtual bool Valid() const = 0;

        virtual void SeekToFirst() = 0;

        virtual void SeekToLast() = 0;

        virtual void Seek(const Slice & target) = 0;

        virtual void Next() = 0;

        virtual void Prev() = 0;

        virtual Slice Key() const = 0;

        virtual Slice Value() const = 0;
    };
}

#endif //LEVIDB_ITERATOR_H
