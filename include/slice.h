#pragma once
#ifndef LEVIDB_SLICE_H
#define LEVIDB_SLICE_H

/*
 * 数据封装类
 */

#include <cassert>
#include <cstring>
#include <string>
#include <type_traits>

namespace levidb {
    class Slice {
    private:
        const char * data_ = "";
        size_t size_ = 0;

    public:
        Slice() = default;

        Slice(const char * d, size_t n) : data_(d), size_(n) {}

        template<typename T>
        Slice(const T * s, size_t n)
                : Slice(reinterpret_cast<const char *>(s), n) {
            static_assert(sizeof(T) == sizeof(char));
        }

        template<typename T, typename = std::enable_if_t<std::is_class<T>::value>>
        Slice(const T & s) : Slice(s.data(), s.size()) {}

        template<typename T, typename = std::enable_if_t<std::is_convertible<T, const char *>::value>>
        Slice(T s) : data_(s), size_(strlen(s)) {}

        template<size_t L>
        explicit Slice(const char (& s)[L]) : data_(s), size_(L - 1) {}

    public:
        // same as STL
        const char * data() const { return data_; }

        // same as STL
        size_t size() const { return size_; }

        std::string ToString() const { return {data_, size_}; }

        const char & operator[](size_t n) const {
            assert(n < size_);
            return data_[n];
        }

        bool operator==(const Slice & another) const {
            return size_ == another.size_ && memcmp(data_, another.data_, size_) == 0;
        }

        bool operator!=(const Slice & another) const { return !operator==(another); }
    };

    inline int SliceCmp(const Slice & a, const Slice & b) {
        int r = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
        if (r == 0) {
            if (a.size() < b.size()) {
                return -1;
            } else if (a.size() == b.size()) {
                return 0;
            } else {
                return +1;
            }
        }
        return r;
    }

    struct SliceComparator {
        using is_transparent = std::true_type;

        bool operator()(const std::string & a, const std::string & b) const {
            return a < b;
        }

        bool operator()(const Slice & a, const Slice & b) const {
            int r = SliceCmp(a, b);
            return r < 0;
        }

        bool operator()(const std::string & a, const Slice & b) const {
            return operator()(Slice(a), b);
        }

        bool operator()(const Slice & a, const std::string & b) const {
            return operator()(a, Slice(b));
        }
    };

    struct SliceHasher {
        std::size_t operator()(const Slice & s) const {
            return std::hash<std::string_view>()({s.data(), s.size()});
        }
    };

    template<typename O, typename S, typename = std::enable_if_t<std::is_same<S, Slice>::value>>
    inline O & operator<<(O & os, const S & s) {
        for (size_t i = 0; i < s.size(); ++i) {
            char c = s[i];
            if (isprint(c)) {
                os << c;
            } else {
                os << '['
                   << static_cast<unsigned int>(*reinterpret_cast<unsigned char *>(&c))
                   << ']';
            }
        }
        return os;
    }
}

#endif //LEVIDB_SLICE_H
