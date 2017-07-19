#ifndef LEVIDB_INDEX_H
#define LEVIDB_INDEX_H

/*
 * 比特退化树
 * https://zhuanlan.zhihu.com/p/27071075
 */

#include "env_io.h"
#include <array>
#include <cassert>
#include <tuple>

namespace LeviDB {
    namespace IndexConst {
        static constexpr int rank = 3; // always >= 3
    }

    class BDNode;

    class BitDegradeTree;

    class CritPtr {
    private:
        void * _ptr;

        friend class BitDegradeTree;

    public:
        CritPtr() noexcept : _ptr(nullptr) {}

        bool isNull() const noexcept {
            return _ptr == nullptr;
        }

        bool isVal() const noexcept {
            assert(!isNull());
            return (reinterpret_cast<uintptr_t>(_ptr) & 1) == 0;
        }

        bool isNode() const noexcept {
            return !isVal();
        }

        void setNull() noexcept {
            _ptr = nullptr;
        }

        void setVal(char * val) noexcept {
            _ptr = reinterpret_cast<void *>(val);
        }

        void setNode(BDNode * node) noexcept {
            _ptr = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(node) | 1);
        }

        const char * asVal() const noexcept {
            assert(isVal());
            return reinterpret_cast<const char *>(_ptr);
        }

        BDNode * asNode() const noexcept {
            assert(isNode());
            return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_ptr) & (UINTPTR_MAX - 1));
        }

        ~CritPtr() noexcept;
    };

    class BDNode {
    public: // for test
        std::array<CritPtr, IndexConst::rank + 1> _ptrs;
        std::array<uint32_t, IndexConst::rank> _diffs;
        std::array<uint8_t, IndexConst::rank> _masks;

    public:
        BDNode() noexcept : _ptrs(), _diffs(), _masks() {}

        ~BDNode() noexcept {}

        bool full() const noexcept { return !_ptrs.back().isNull(); }

        int size() const noexcept;

        auto getDiffLess() const noexcept;
    };

    class BDAddrTranslator {

    };

    class BitDegradeTree {
    private:
        BDNode _node;

    public:
        BDNode * _root; // for test

        BitDegradeTree() noexcept : _node(), _root(&_node) {}

        ~BitDegradeTree() noexcept {}

        void insert(char * kv) noexcept;

        const char * find(const char * k) const noexcept;

        void remove(const char * k) noexcept;

        size_t size(const BDNode * node) const noexcept;

    private:
        std::tuple<int/* idx */, bool/* direct */, int/* size */>
        findBestMatch(const BDNode * node, const char * k) const noexcept;

        void combatInsert(const char * opponent, char * kv) noexcept;

        void nodeInsert(BDNode * node, int replace_idx, bool replace_direct,
                        bool direct, uint32_t diff_at, uint8_t mask, char * kv,
                        int size) noexcept;

        void makeRoom(BDNode * parent) noexcept;

        void makeRoomPush(BDNode * parent, BDNode * child, int idx, bool direct) noexcept;

        void makeNewRoom(BDNode * parent, int idx) noexcept;

        void nodeRemove(BDNode * node, int idx, bool direct, int size) noexcept;

        void tryMerge(BDNode * parent, BDNode * child,
                      int idx, bool direct, int parent_size,
                      int child_size) noexcept;

        // 禁止复制
        BitDegradeTree(const BitDegradeTree &);

        void operator=(const BitDegradeTree &);
    };
}

#endif //LEVIDB_INDEX_H