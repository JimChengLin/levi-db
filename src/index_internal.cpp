#ifndef __linux__

#include <machine/endian.h>

#define PLATFORM_IS_LITTLE_ENDIAN (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#else

#include <endian.h>

#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#include "crc32c.h"
#include "index_internal.h"

namespace levidb8 {
    static_assert(PLATFORM_IS_LITTLE_ENDIAN, "cannot mix marks");

    const uint32_t *
    unfairMinElem(const uint32_t * cbegin, const uint32_t * cend, const BDNode * node,
                  std::array<uint32_t, kRank> & calc_cache) noexcept {
        assert(cbegin != cend);
        const uint32_t * res = cbegin;
        uint64_t res_cmp = mixMarks(*res, node->immut_masks()[res - node->immut_diffs().cbegin()]);

        const uint32_t * cursor = cbegin;
        while (++cursor != cend) {
            uint64_t cursor_cmp = mixMarks(*cursor, node->immut_masks()[cursor - node->immut_diffs().cbegin()]);

            if (cursor_cmp < res_cmp) {
                auto idx = static_cast<uint16_t>(cursor - cbegin);
                calc_cache[idx - 1] = static_cast<uint16_t>(res - cbegin);
                res = cursor;
                res_cmp = cursor_cmp;
                calc_cache[idx] = idx;
            }
        }
        return res;
    }

    bool BDEmpty::verify() const noexcept {
        return crc32c::verify(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum), &_checksum[0]);
    }

    void BDEmpty::updateChecksum() noexcept {
        uint32_t checksum = crc32c::value(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum));
        memcpy(&_checksum[0], &checksum, sizeof(checksum));
    }

    bool CritPtr::isNull() const noexcept {
        return _offset == kDiskNull;
    }

    bool CritPtr::isData() const noexcept {
        assert(!isNull());
        return _offset % kPageSize != 0;
    }

    bool CritPtr::isNode() const noexcept {
        return !isData();
    }

    void CritPtr::setNull() noexcept {
        _offset = kDiskNull;
    }

    void CritPtr::setData(uint32_t offset) noexcept {
        _offset = offset;
        assert(isData());
    }

    void CritPtr::setData(OffsetToData data) noexcept {
        setData(data.val);
    }

    void CritPtr::setNode(uint32_t offset) noexcept {
        _offset = offset;
        assert(isNode());
    }

    void CritPtr::setNode(OffsetToNode node) noexcept {
        setNode(node.val);
    }

    OffsetToData CritPtr::asData() const noexcept {
        assert(isData());
        return {_offset};
    }

    OffsetToNode CritPtr::asNode() const noexcept {
        assert(isNode());
        return {_offset};
    }

    bool BDNode::full() const noexcept {
        return !_ptrs.back().isNull();
    }

    size_t BDNode::size() const noexcept {
        if (full()) {
            return _ptrs.size();
        }
        size_t lo = 0;
        size_t hi = kRank;
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (_ptrs[mid].isNull()) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    bool BDNode::verify() const noexcept {
        return (_padding_[0] | _padding_[1]) == 0
               && crc32c::verify(reinterpret_cast<const char *>(this), offsetof(BDNode, _checksum), &_checksum[0]);
    }

    void BDNode::updateChecksum() noexcept {
        uint32_t checksum = crc32c::value(reinterpret_cast<const char *>(this), offsetof(BDNode, _checksum));
        memcpy(&_checksum[0], &checksum, sizeof(checksum));
    }
}