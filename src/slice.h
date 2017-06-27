#ifndef LEVIDB_SLICE_H
#define LEVIDB_SLICE_H

#include <cassert>
#include <cstring>
#include <string>

namespace LeviDB {
    class Slice {
    public:
        Slice() noexcept : _data(""), _size(0) {}

        Slice(const char * d, size_t n) noexcept : _data(d), _size(n) {}

        Slice(const std::string & s) noexcept : _data(s.data()), _size(s.size()) {}

        Slice(const char * s) noexcept : _data(s), _size(strlen(s)) {}

        const char * data() const noexcept { return _data; }

        size_t size() const noexcept { return _size; }

        bool empty() const noexcept { return _size == 0; }

        char operator[](size_t n) const noexcept {
            assert(n < _size);
            return _data[n];
        }

        void clear() noexcept {
            _data = "";
            _size = 0;
        }

        void removePrefix(size_t n) noexcept {
            assert(n <= _size);
            _data += n;
            _size -= n;
        }

        std::string toString() const noexcept { return std::string(_data, _size); }

        inline int compare(const Slice & b) const noexcept;

        bool startsWith(const Slice & x) const noexcept {
            return ((_size >= x._size) &&
                    (memcmp(_data, x._data, x._size) == 0));
        }

    private:
        const char * _data;
        size_t _size;
    };

    inline int Slice::compare(const Slice & b) const noexcept {
        const size_t min_len = std::min(_size, b._size);
        int r = memcmp(_data, b._data, min_len);
        if (r == 0) {
            if (_size < b._size) r = -1;
            else if (_size > b._size) r = +1;
        }
        return r;
    }

    inline bool operator==(const Slice & x, const Slice & y) noexcept {
        return ((x.size() == y.size()) &&
                (memcmp(x.data(), y.data(), x.size()) == 0));
    }

    inline bool operator!=(const Slice & x, const Slice & y) noexcept {
        return !(x == y);
    }
} //namespace LeviDB

#endif //LEVIDB_SLICE_H