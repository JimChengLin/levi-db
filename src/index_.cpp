#include <sys/mman.h>

#include "config.h"
#include "exception.h"
#include "index_internal.h"

namespace levidb8 {
    BDNode * BitDegradeTree::offToMemNode(OffsetToNode node) const {
        auto * mem_node = reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region())
                                                     + node.val);
#ifndef __linux__
        static_assert(MINCORE_INCORE == 1, "*nix conflict");
#else
        unsigned
#endif
        char vec[1]{};
        if (mincore(mem_node, kPageSize, vec) != 0) { throw Exception::corruptionException("mincore fail"); };
        if ((vec[0] & 1) != 1 && !mem_node->verify()) {
            throw Exception::corruptionException("i-node checksum mismatch");
        };
        return mem_node;
    }

    BDNode * BitDegradeTree::offToMemNodeUnchecked(OffsetToNode node) const noexcept {
        return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region()) + node.val);
    }

    ReadWriteLock * BitDegradeTree::offToNodeLock(OffsetToNode node) const noexcept {
        assert(_dst.immut_length() == _node_locks.size() * kPageSize);
        return _node_locks[node.val / kPageSize].get();
    }

    BDEmpty * BitDegradeTree::offToMemEmpty(OffsetToEmpty empty) const {
        auto * mem_empty = reinterpret_cast<BDEmpty *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region())
                                                       + empty.val);
        if (!mem_empty->verify()) {
            throw Exception::corruptionException("e-node checksum mismatch");
        };
        return mem_empty;
    }

    OffsetToNode BitDegradeTree::mallocNode() {
        std::lock_guard<std::mutex> guard(_allocate_lock);
        if (_empty.val == kDiskNull) {
            throw ExpandControlledException();
        }
        OffsetToNode res{_empty.val};
        _empty = offToMemEmpty(_empty)->immut_next();
        return res;
    }

    void BitDegradeTree::freeNode(OffsetToNode node) noexcept {
        std::lock_guard<std::mutex> guard(_allocate_lock);
        freeNodeUnlocked(node);
    }

    void BitDegradeTree::freeNodeUnlocked(OffsetToNode node) noexcept {
        auto empty = new(offToMemNodeUnchecked(node)) BDEmpty;
        empty->mut_next() = _empty;
        empty->updateChecksum();
        _empty.val = node.val;
    }

    class MatcherDebugOffset : public Matcher { // 仅用于 debug
    private:
        uint32_t _val; // 相当于 char[4]

    public:
        explicit MatcherDebugOffset(OffsetToData data) noexcept : _val(data.val) {}

        ~MatcherDebugOffset() noexcept override = default;

        char operator[](size_t idx) const override {
            return reinterpret_cast<const char *>(&_val)[idx];
        };

        bool operator==(const Matcher & another) const override {
            return toSlice() == another.toSlice();
        };

        bool operator==(const Slice & another) const override {
            return toSlice() == another;
        };

        size_t size() const override {
            return sizeof(_val);
        };

        Slice toSlice() const override {
            return Slice(reinterpret_cast<const char *>(&_val), sizeof(_val));
        };
    };

    std::unique_ptr<Matcher> BitDegradeTree::offToMatcher(OffsetToData data) const {
        return std::make_unique<MatcherDebugOffset>(data);
    }

    class MatcherDebugSlice : public Matcher {
    private:
        Slice _slice;

    public:
        explicit MatcherDebugSlice(Slice slice) noexcept : _slice(std::move(slice)) {}

        ~MatcherDebugSlice() noexcept override = default;

        char operator[](size_t idx) const override {
            return _slice[idx];
        };

        bool operator==(const Matcher & another) const override {
            return toSlice() == another.toSlice();
        };

        bool operator==(const Slice & another) const override {
            return toSlice() == another;
        };

        size_t size() const override {
            return _slice.size();
        };

        Slice toSlice() const override {
            return _slice;
        };
    };

    std::unique_ptr<Matcher> BitDegradeTree::sliceToMatcher(const Slice & slice) const noexcept {
        return std::make_unique<MatcherDebugSlice>(slice);
    }
}