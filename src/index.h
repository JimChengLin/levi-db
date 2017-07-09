#ifndef LEVIDB_INDEX_H
#define LEVIDB_INDEX_H

/*
 * 比特退化树
 * https://zhuanlan.zhihu.com/p/27071075
 */

#include <cassert>
#include <cstdint>

namespace LeviDB {
    namespace IndexConst {
        static constexpr int rank = 3;
    }

    template<typename K>
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

            void setVal(K * val) noexcept {
                _ptr = val;
            }

            void setNode(BDNode * node) noexcept {
                _ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(node) | 1);
            }

            K * asVal() const noexcept {
                assert(isVal());
                return _ptr;
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

        CritPointer _ptrs[IndexConst::rank + 1];
        uint32_t _diffs[IndexConst::rank];
        uint8_t _masks[IndexConst::rank];

    public:
        BDNode() noexcept : _ptrs(), _diffs(), _masks() {}

        ~BDNode() noexcept {
            for (CritPointer ptr:_ptrs) {
                if (ptr.isNull()) {
                    break;
                }
                ptr.~CritNodePtr();
            }
        }

        bool full() const noexcept {
            return !_ptrs[IndexConst::rank].isNull();
        }
    };

    template<typename K>
    class BitDegradeTree {
    private:
        BDNode<K> _node;
        BDNode<K> * _root;

    public:
        BitDegradeTree() noexcept : _node(), _root(&_node) {}

        ~BitDegradeTree() noexcept {}

        void insert(K * val) noexcept {

        }
    };
}

#endif //LEVIDB_INDEX_H