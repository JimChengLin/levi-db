#pragma once
#ifndef LEVIDB8_SIMPLE_ITERATOR_H
#define LEVIDB8_SIMPLE_ITERATOR_H

namespace levidb8 {
    template<typename T, typename I = bool>
    class SimpleIterator {
    public:
        SimpleIterator() noexcept = default;

    public:
        virtual ~SimpleIterator() noexcept = default;

        virtual bool valid() const = 0;

        virtual T item() const = 0;

        virtual void next() = 0;

        virtual I info() const { return {}; }
    };
}

#endif //LEVIDB8_SIMPLE_ITERATOR_H
