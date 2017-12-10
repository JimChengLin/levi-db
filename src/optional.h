#pragma once
#ifndef LEVIDB8_OPTIONAL_H
#define LEVIDB8_OPTIONAL_H

/*
 * 最小 c++ std::optional 模拟
 */

#include <cassert>
#include <type_traits>
#include <utility>

namespace levidb8 {
    template<typename T>
    class Optional {
    private:
        typename std::aligned_storage<sizeof(T), alignof(T)>::type _obj[1];
        bool _valid = false;

    public:
        Optional() noexcept = default;

        ~Optional() noexcept {
            if (_valid) {
                (*get()).T::~T();
            }
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

    public:
        Optional(const Optional &) noexcept = delete;

        void operator=(const Optional &) noexcept = delete;

        Optional(Optional &&) noexcept = delete;

        void operator=(Optional &&) noexcept = delete;
    };
}

#endif //LEVIDB8_OPTIONAL_H
