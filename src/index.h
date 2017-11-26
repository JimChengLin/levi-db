#pragma once
#ifndef LEVIDB8_INDEX_H
#define LEVIDB8_INDEX_H

/*
 * 比特退化树
 * https://zhuanlan.zhihu.com/p/27071075
 */

#ifndef __clang__

#include <mutex>

#endif

#include "config.h"
#include "env_io.h"
#include "env_thread.h"
#include "iterator.h"

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

    class Matcher;

    class IndexFullControlledException : public std::exception {
    };

    class BitDegradeTree {
    protected:
        MmapFile _dst;
        OffsetToEmpty _empty{kDiskNull};
        mutable ReadWriteLock _expand_lock;
        std::mutex _allocate_lock;
        std::vector<std::unique_ptr<ReadWriteLock>> _node_locks;

        class BitDegradeTreeIterator;

    public:
        explicit BitDegradeTree(const std::string & fname);

        BitDegradeTree(const std::string & fname, OffsetToEmpty empty);

    public:
        // 返回最接近 k 的结果, 调用者需要二次检查是否真正匹配
        // kDiskNull 说明未找到
        OffsetToData find(const Slice & k) const;

        // 当有 compress record 时, size 不准确, 仅用作内部测试
        size_t size() const;

        void insert(const Slice & k, OffsetToData v);

        void remove(const Slice & k, OffsetToData v);

        // Iterator 的方法有可能抛出异常
        std::unique_ptr<Iterator<Slice/* usr */, OffsetToData>>
        scan() const noexcept;

        void sync() { _dst.sync(); };

    private:
        size_t size(OffsetToNode node) const;

        std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
        findBestMatch(const BDNode * node, const Slice & k, USR * reveal_info = nullptr) const noexcept;

        bool combatInsert(const Slice & opponent, OffsetToData from, const Slice & k, OffsetToData v);

        void nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                        bool direct, uint32_t diff_at, uint8_t mask, OffsetToData v,
                        size_t size) noexcept;

        void makeRoom(BDNode * parent);

        void nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept;

        void tryMerge(BDNode * parent, size_t idx, bool direct, size_t parent_size,
                      BDNode * child, size_t child_size) noexcept;

    protected:
        BDNode * offToMemNode(OffsetToNode node) const;

        BDNode * offToMemNodeUnchecked(OffsetToNode node) const noexcept;

        ReadWriteLock * offToNodeLock(OffsetToNode node) const noexcept;

        BDEmpty * offToMemEmpty(OffsetToEmpty empty) const;

        OffsetToNode mallocNode();

        void freeNode(OffsetToNode node) noexcept;

        void freeNodeUnlocked(OffsetToNode node) noexcept;

        // 继承时覆盖
        virtual std::unique_ptr<Matcher> offToMatcher(OffsetToData data) const noexcept;

        virtual std::unique_ptr<Matcher> sliceToMatcher(const Slice & slice) const noexcept;
    };
}

#endif //LEVIDB8_INDEX_H
