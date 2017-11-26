#pragma once
#ifndef LEVIDB8_ITERATOR_H
#define LEVIDB8_ITERATOR_H

/*
 * 迭代器接口
 */

#include "slice.h"

namespace levidb8 {
    template<typename K, typename V>
    class Iterator {
    public:
        Iterator() noexcept = default;

    public:
        virtual ~Iterator() noexcept = default;

        virtual bool valid() const = 0;

        virtual void seekToFirst() = 0;

        virtual void seekToLast() = 0;

        virtual void seek(const Slice & target) = 0;

        virtual void seekForPrev(const Slice & target) = 0;

        virtual void next() = 0;

        virtual void prev() = 0;

        virtual K key() const = 0;

        virtual V value() const = 0;
    };

    template<typename T>
    class SimpleIterator {
    public:
        SimpleIterator() noexcept = default;

    public:
        virtual ~SimpleIterator() noexcept = default;

        virtual void prepare() = 0;

        virtual void next() = 0;

        virtual bool valid() const = 0;

        virtual T item() const = 0;
    };
}

#endif //LEVIDB8_ITERATOR_H
