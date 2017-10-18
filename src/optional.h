#ifndef LEVIDB_OPTIONAL_H
#define LEVIDB_OPTIONAL_H

/*
 * 最小 c++ std::optional 模拟
 */

#include <cassert>
#include <utility>

#include "util.h"

namespace LeviDB {
    template<typename T>
    class Optional {
    private:
        size_t _obj[(sizeof(T) + sizeof(size_t) - 1) / sizeof(size_t)]{};
        bool _valid = false;

    public:
        Optional() noexcept = default;

        DELETE_MOVE(Optional);
        DELETE_COPY(Optional);

        ~Optional() noexcept {
            if (_valid) (*get()).T::~T();
        }

    public:
        bool valid() const noexcept {
            return _valid;
        }

        T * operator->() noexcept {
            assert(valid());
            return reinterpret_cast<T *>(_obj);
        }

        const T * operator->() const noexcept {
            assert(valid());
            return reinterpret_cast<const T *>(_obj);
        }

        T * get() noexcept {
            return operator->();
        }

        const T * get() const noexcept {
            return operator->();
        }

        void reset() noexcept {
            assert(valid());
            (*get()).~T();
            _valid = false;
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