#ifndef LEVIDB_INDEX_H
#define LEVIDB_INDEX_H

/*
 * 比特退化树
 * https://zhuanlan.zhihu.com/p/27071075
 */

#include "slice.h"
#include "util.h"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <type_traits>

namespace LeviDB {
    namespace IndexConst {
        static constexpr int rank = 3;
    }

    template<typename T>
    class BDNode {
    public:
        class CritPointer {
        private:
            void * _ptr;

        public:
            CritPointer() noexcept : _ptr(nullptr) {}

            bool isNull() const noexcept {
                return _ptr == nullptr;
            }

            bool isVal() const noexcept {
                assert(!isNull());
                return (reinterpret_cast<uintptr_t>(this) & 1) == 0;
            }

            bool isNode() const noexcept {
                assert(!isNull());
                return !isVal();
            }

            void setVal(T * val) noexcept {
                _ptr = val;
            }

            void setNode(BDNode * node) noexcept {
                _ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(node) | 1);
            }

            T * asVal() const noexcept {
                assert(isVal());
                return reinterpret_cast<T *>(_ptr);
            }

            BDNode * asNode() const noexcept {
                assert(isNode());
                return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(this) & (UINTPTR_MAX - 1));
            }

            ~CritPointer() noexcept {
                if (!isNull()) {
                    if (isVal()) {
                        delete asVal();
                    } else {
                        delete asNode();
                    }
                }
            }
        };

        std::array<CritPointer, IndexConst::rank + 1> _ptrs;
        std::array<uint32_t, IndexConst::rank> _diffs;
        std::array<uint8_t, IndexConst::rank> _masks;

        static_assert(std::is_standard_layout<decltype(_ptrs)>::value, "layout error");
        static_assert(std::is_standard_layout<decltype(_diffs)>::value, "layout error");
        static_assert(std::is_standard_layout<decltype(_masks)>::value, "layout error");

    public:
        BDNode() noexcept : _ptrs(), _diffs(), _masks() {
            static_assert(std::is_standard_layout<decltype(this)>::value, "layout error");
        }

        ~BDNode() noexcept {}

        bool full() const noexcept { return !_ptrs.back().isNull(); }
    };

    template<typename T>
    class BitDegradeTree {
    private:
        BDNode<T> _node;
        BDNode<T> * _root;

    public:
        BitDegradeTree() noexcept : _node(), _root(&_node) {}

        ~BitDegradeTree() noexcept {}

        void insert(const Slice & key, T * val) noexcept {

        }

    private:
        BDNode<T> * findBestNode(const Slice & key) const noexcept {
            BDNode<T> * cursor = _root;
            while (cursor->full()) {

            }
            return cursor;
        }

        size_t findBestPos(BDNode<T> * node, const Slice & key) const noexcept {
            const uint32_t * cbegin = node->_diffs.cbegin();
            const uint32_t * cend = node->_diffs.cend();

            while (true) {
                if (cbegin == cend) {
                    break;
                }

                auto min_max = std::minmax_element(cbegin, cend);
                const uint32_t * min_it = min_max.first;
                const uint32_t * max_it = min_max.second;
                const uint32_t diff_at = *min_it;

                // go left or right
                uint8_t crit_byte = key.size() > diff_at ? char_be_uint8(key[diff_at]) : static_cast<uint8_t>(0);
                bool direct = (static_cast<uint8_t>(1) + (node->_masks[diff_at] | crit_byte)) >> 8;
                if (direct == 0) { // left
                    cend = min_it;
                    if (max_it <= min_it) {
                        cbegin = max_it;
                    }
                } else { // right
                    assert(direct == 1);
                    cbegin = min_it + 1;
                    if (max_it >= min_it) {
                        cend = max_it + 1;
                    }
                }
            }

            return node->_diffs.cbegin() - cbegin;
        }
    };
}

#endif //LEVIDB_INDEX_H