#pragma once
#ifndef LEVIDB8_ITERATOR_H
#define LEVIDB8_ITERATOR_H

/*
 * 迭代器接口
 */

namespace levidb8 {
    template<typename K, typename V, typename I = bool>
    class Iterator {
    public:
        Iterator() noexcept = default;

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

        virtual I info() const { return {}; }
    };
}

#endif //LEVIDB8_ITERATOR_H
