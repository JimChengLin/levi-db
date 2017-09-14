#ifndef LEVIDB_OPTIONAL_H
#define LEVIDB_OPTIONAL_H

/*
 * 最小 c++ std:optional 模拟
 */

#include <cassert>
#include <utility>

#include "util.h"

namespace LeviDB {
    template<typename T>
    class Optional {
    private:
        char _obj[sizeof(T)]{};
        bool _valid = false;

    public:
        Optional() noexcept = default;

        DELETE_MOVE(Optional);
        DELETE_COPY(Optional);

        ~Optional() noexcept {
            if (_valid) value().~T();
        }

    public:
        bool valid() const noexcept {
            return _valid;
        }

        T * operator->() noexcept {
            assert(valid());
            return reinterpret_cast<T *>(_obj);
        }

        void reset() noexcept {
            assert(valid());
            _valid = false;
            value().~T();
        }

        template<typename ...PARAMS>
        void build(PARAMS && ... params) {
            assert(!valid());
            new(_obj) T(std::forward<PARAMS>(params)...);
            _valid = true;
        }
    };
}

#endif //LEVIDB_OPTIONAL_H