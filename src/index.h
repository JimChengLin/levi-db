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
#include <utility>

namespace LeviDB {
    namespace IndexConst {
        static constexpr int rank = 3;
    }

    template<typename T>
    class BDNode {
    public:
        class CritPtr {
        private:
            void * _ptr;

        public:
            CritPtr() noexcept : _ptr(nullptr) {}

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

            ~CritPtr() noexcept {
                if (!isNull()) {
                    if (isVal()) {
                        delete asVal();
                    } else {
                        delete asNode();
                    }
                }
            }
        };

        std::array<CritPtr, IndexConst::rank + 1> _ptrs;
        std::array<uint32_t, IndexConst::rank> _diffs;
        std::array<uint8_t, IndexConst::rank> _masks;

        static_assert(std::is_standard_layout<decltype(_ptrs)>::value, "standard layout for mmap");
        static_assert(std::is_standard_layout<decltype(_diffs)>::value, "standard layout for mmap");
        static_assert(std::is_standard_layout<decltype(_masks)>::value, "standard layout for mmap");

    public:
        BDNode() noexcept : _ptrs(), _diffs(), _masks() {
            static_assert(std::is_standard_layout<decltype(this)>::value, "standard layout for mmap");
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
        std::pair<size_t, bool> findBestPos(BDNode<T> * node, const Slice & key) const noexcept {
            const uint32_t * cbegin = node->_diffs.cbegin();
            const uint32_t * cend;

            int ptr_cnt = 0;
            for (BDNode<T>::CritPtr ptr:node->_ptrs) {
                if (!ptr.isNull()) {
                    ++ptr_cnt;
                } else {
                    break;
                }
            }
            if (ptr_cnt <= 1) {
                return {0, false};
            } else {
                cend = &node->_diffs[ptr_cnt - 1];
            }

            while (true) {
                const uint32_t * min_it = std::min_element(cbegin, cend,
                                                           [&node](const uint32_t & a, const uint32_t & b) {
                                                               if (a < b) {
                                                                   return true;
                                                               } else if (a == b) {
                                                                   return node->_masks[node->_diffs.cbegin() - &a] <
                                                                          node->_masks[node->_diffs.cbegin() - &b];
                                                               } else {
                                                                   return false;
                                                               }
                                                           });
                uint32_t diff_at = *min_it;

                // go left or right
                uint8_t crit_byte = key.size() > diff_at ? char_be_uint8(key[diff_at]) : static_cast<uint8_t>(0);
                bool false_left_true_right = (static_cast<uint8_t>(1) + (node->_masks[diff_at] | crit_byte)) >> 8;
                if (!false_left_true_right) {
                    cend = min_it;
                } else {
                    cbegin = min_it + 1;
                }

                if (cbegin == cend) {
                    return {node->_diffs.cbegin() - min_it, false_left_true_right};
                }
            }
        }
    };
}

#endif //LEVIDB_INDEX_H