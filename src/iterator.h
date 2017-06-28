#ifndef LEVIDB_ITERATOR_H
#define LEVIDB_ITERATOR_H

#include "slice.h"

namespace LeviDB {
    class Iterator {
    public:
        Iterator() {};

        virtual ~Iterator() {};

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
}

#endif //LEVIDB_ITERATOR_H