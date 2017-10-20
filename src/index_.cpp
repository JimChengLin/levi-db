#include "index.h"
#include "crc32c.h"
#include "exception.h"

namespace LeviDB {
    static constexpr uint32_t page_size_ = 4096;

    size_t BDNode::size() const noexcept {
        if (full()) {
            return _ptrs.size();
        }
        size_t cnt = 0;
        for (CritPtr ptr:_ptrs) {
            if (!ptr.isNull()) {
                ++cnt;
            } else {
                break;
            }
        }
        return cnt;
    }

    bool BDEmpty::verify() const noexcept {
        return CRC32C::verify(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum), &_checksum[0]);
    }

    void BDEmpty::updateChecksum() noexcept {
        uint32_t checksum = CRC32C::value(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum));
        memcpy(&_checksum[0], &checksum, sizeof(checksum));
    }

    bool BDNode::verify() const noexcept {
        return _padding_[0] == 0 && _padding_[1] == 0
               && CRC32C::verify(reinterpret_cast<const char *>(this), offsetof(BDNode, _checksum), &_checksum[0]);
    }

    void BDNode::updateChecksum() noexcept {
        uint32_t checksum = CRC32C::value(reinterpret_cast<const char *>(this), offsetof(BDNode, _checksum));
        memcpy(&_checksum[0], &checksum, sizeof(checksum));
    }

    BDNode * BitDegradeTree::offToMemNode(OffsetToNode node) const {
        assert(sysconf(_SC_PAGESIZE) == page_size_);
        auto * mem_node = reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region())
                                                     + node.val);
        char vec[1]{};
        if (mincore(mem_node, page_size_, vec) != 0) { throw Exception::corruptionException("mincore fail"); };
        if ((vec[0] & 1) != 1 && !mem_node->verify()) {
            throw Exception::corruptionException("i-node checksum mismatch");
        };
        return mem_node;
    }

    BDNode * BitDegradeTree::offToMemNodeUnchecked(OffsetToNode node) const noexcept {
        return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region()) + node.val);
    }

    BDEmpty * BitDegradeTree::offToMemEmpty(OffsetToEmpty empty) const {
        auto * mem_empty = reinterpret_cast<BDEmpty *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region())
                                                       + empty.val);
        if (!mem_empty->verify()) {
            throw Exception::corruptionException("e-node checksum mismatch");
        };
        return mem_empty;
    }

    class MatcherDebugOffset : public Matcher { // 仅用于 debug, 真正的实现由继承覆盖
    private:
        uint32_t _val; // 相当于 char[4]

    public:
        explicit MatcherDebugOffset(OffsetToData data) noexcept : _val(data.val) {}

        DEFAULT_MOVE(MatcherDebugOffset);
        DEFAULT_COPY(MatcherDebugOffset);

        ~MatcherDebugOffset() noexcept override = default;

        char operator[](size_t idx) const override {
            return reinterpret_cast<const char *>(&_val)[idx];
        };

        bool operator==(const Matcher & another) const override {
            return toString() == another.toString();
        };

        bool operator==(const Slice & another) const override {
            return toString() == another.toString();
        };

        size_t size() const override {
            return sizeof(_val);
        };

        std::string toString() const override {
            return std::string(reinterpret_cast<const char *>(&_val), sizeof(_val));
        };
    };

    std::unique_ptr<Matcher> BitDegradeTree::offToMatcher(OffsetToData data) const noexcept {
        return std::make_unique<MatcherDebugOffset>(data);
    }

    class MatcherDebugSlice : public Matcher {
    private:
        Slice _slice;

    public:
        explicit MatcherDebugSlice(const Slice & slice) noexcept : _slice(slice) {}

        DEFAULT_MOVE(MatcherDebugSlice);
        DEFAULT_COPY(MatcherDebugSlice);

        ~MatcherDebugSlice() noexcept override = default;

        char operator[](size_t idx) const override {
            return _slice[idx];
        };

        bool operator==(const Matcher & another) const override {
            return toString() == another.toString();
        };

        bool operator==(const Slice & another) const override {
            return toString() == another.toString();
        };

        size_t size() const override {
            return _slice.size();
        };

        std::string toString() const override {
            return _slice.toString();
        };
    };

    std::unique_ptr<Matcher> BitDegradeTree::sliceToMatcher(const Slice & slice) const noexcept {
        return std::make_unique<MatcherDebugSlice>(slice);
    }

    OffsetToNode BitDegradeTree::mallocNode() {
        if (_empty.val == IndexConst::disk_null_) {
            OffsetToNode res{static_cast<uint32_t>(_dst.immut_length())};
            _dst.grow();
            return res;
        }
        OffsetToNode res{_empty.val};
        _empty = offToMemEmpty(_empty)->immut_next();
        return res;
    }

    void BitDegradeTree::freeNode(OffsetToNode node) noexcept {
        auto empty = new(offToMemNodeUnchecked(node)) BDEmpty;
        empty->mut_next() = _empty;
        empty->updateChecksum();
        _empty.val = node.val;
    }
}