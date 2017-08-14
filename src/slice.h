#ifndef LEVIDB_SLICE_H
#define LEVIDB_SLICE_H

/*
 * 数据简单封装类
 * 只保存对源数据的指针和长度
 *
 * 注意:
 * 为了跟 std::string 有更简单的交互, _data 类型为 const char *
 * 但绝大多数情况下应该被转化成无符号的 uint8_t *
 * 表明这仅仅是一段数据而非 char 组成的 string
 *
 * char 和 uint8_t 在 static_cast 和 equality 判断的时候
 * 常有不符合直觉的差异
 *
 * steal from leveldb
 */

#include <cassert>
#include <cstring>
#include <string>

namespace LeviDB {
    class Slice {
    private:
        const char * _data = "";
        size_t _size = 0;

    public:
        Slice() noexcept = default;

        Slice(const char * d, size_t n) noexcept : _data(d), _size(n) {}

        template<typename T>
        Slice(T * s, size_t n) noexcept
                :Slice(reinterpret_cast<const char *>(s), n) {
            static_assert(sizeof(T) == sizeof(char), "args mismatch");
        }

        Slice(const std::string & s) noexcept : _data(s.data()), _size(s.size()) {}

        Slice(const char * s) noexcept : _data(s), _size(strlen(s)) {}

        const char * data() const noexcept { return _data; }

        size_t size() const noexcept { return _size; }

        std::string toString() const noexcept { return std::string(_data, _size); }

        char operator[](size_t n) const noexcept {
            assert(n < _size);
            return _data[n];
        }

        bool operator==(const Slice & another) const noexcept {
            return _size == another._size && memcmp(_data, another._data, _size) == 0;
        }
    };

    struct SliceComparator {
        using is_transparent = std::true_type;

        bool operator()(const std::string & a, const std::string & b) const noexcept {
            return a < b;
        }

        bool operator()(const Slice & a, const Slice & b) const noexcept {
            int r = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
            if (r == 0) {
                return a.size() < b.size();
            }
            return r < 0;
        }

        bool operator()(const std::string & a, const Slice & b) const noexcept {
            return operator()(Slice(a), b);
        }

        bool operator()(const Slice & a, const std::string & b) const noexcept {
            return operator()(a, Slice(b));
        }
    };
}

#endif //LEVIDB_SLICE_H