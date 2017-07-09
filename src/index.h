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
        class CritNodePtr {
        private:
            void * _ptr;

        public:
            CritNodePtr() noexcept : _ptr(nullptr) {}

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

            K * asVal() const noexcept {
                assert(isVal());
                return _ptr;
            }

            BDNode * asNode() const noexcept {
                assert(isNode());
                return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(this) & (INTPTR_MAX - 1));
            }

            void setVal(K * val) noexcept {
                _ptr = val;
            }

            void setNode(BDNode * node) noexcept {
                _ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(node) | 1);
            }

            ~CritNodePtr() noexcept {
                if (!isNull()) {
                    if (isVal()) {
                        delete asVal();
                    } else {
                        delete asNode();
                    }
                }
            }
        };

        CritNodePtr _nodes[IndexConst::rank + 1];
        uint32_t _diffs[IndexConst::rank];
        uint8_t _mask[IndexConst::rank];

        BDNode() noexcept : _nodes(), _diffs(), _mask() {}

        ~BDNode() noexcept {
            for (CritNodePtr ptr:_nodes) {
                if (ptr.isNull()) {
                    break;
                }
                ptr.~CritNodePtr();
            }
        }
    };

    template<typename T>
    class BitDegradeTree {

    };
}

#endif //LEVIDB_INDEX_H