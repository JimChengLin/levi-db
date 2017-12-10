#pragma once
#ifndef LEVIDB8_INDEX_H
#define LEVIDB8_INDEX_H

/*
 * 比特退化树
 * https://zhuanlan.zhihu.com/p/27071075
 */

#include <mutex>

#include "../include/iterator.h"
#include "config.h"
#include "env_io.h"
#include "env_thread.h"

namespace levidb8 {
    struct OffsetToNode {
        uint32_t val;
    };

    struct OffsetToEmpty {
        uint32_t val;
    };

    struct OffsetToData {
        uint32_t val;
    };

    class BDNode;

    class BDEmpty;

    class UniversalStringRepresentation;

    using USR = UniversalStringRepresentation;

    class IndexFullControlledException : public std::exception {
    };

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    class BitDegradeTree {
    protected:
        MmapFile _dst;
        OffsetToEmpty _empty{kDiskNull};
        mutable ReadWriteLock _expand_lock;
        std::mutex _allocate_lock;
        std::vector<std::unique_ptr<ReadWriteLock>> _node_locks;
        mutable CACHE _cache{};

    public:
        explicit BitDegradeTree(const std::string & fname);

        BitDegradeTree(const std::string & fname, OffsetToEmpty empty);

    public:
        // 返回最接近 k 的结果, 需要二次检查是否真正匹配
        // kDiskNull 说明未找到
        OffsetToData find(const Slice & k) const;

        // 当有 compress record 时, size 不准确, 仅用作内部测试
        size_t size() const;

        void insert(const Slice & k, OffsetToData v);

        void remove(const Slice & k, OffsetToData v);

        void sync() { _dst.sync(); };

        // 相当于 Iterator<Slice/* usr */, OffsetToData>
        class BDIterator;

    private:
        size_t size(OffsetToNode node) const;

        std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
        findBestMatch(const BDNode * node, const Slice & k, USR * reveal_info = nullptr) const noexcept;

        bool combatInsert(const Slice & opponent, OffsetToData from, const Slice & k, OffsetToData v);

        void nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                        bool direct, uint16_t diff, OffsetToData v,
                        size_t size) noexcept;

        void makeRoom(BDNode * parent);

        void nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept;

        void tryMerge(BDNode * parent, size_t idx, bool direct, size_t parent_size,
                      BDNode * child, size_t child_size) noexcept;

    private:
        BDNode * offToMemNode(OffsetToNode node) const noexcept;

        ReadWriteLock * offToNodeLock(OffsetToNode node) const noexcept;

        BDEmpty * offToMemEmpty(OffsetToEmpty empty) const;

        OffsetToNode mallocNode();

        void freeNode(OffsetToNode node) noexcept;

        void freeNodeUnlocked(OffsetToNode node) noexcept;
    };
}

#endif //LEVIDB8_INDEX_H
