#include <algorithm>
#include <nmmintrin.h>

#include "crc32c.h"
#include "index_internal.h"

namespace levidb8 {
    bool BDEmpty::verify() const noexcept {
        return crc32c::value(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum)) == _checksum;
    }

    void BDEmpty::updateChecksum() noexcept {
        _checksum = crc32c::value(reinterpret_cast<const char *>(this), offsetof(BDEmpty, _checksum));
    }

    bool CritPtr::isNull() const noexcept {
        return _offset == kDiskNull;
    }

    bool CritPtr::isData() const noexcept {
        assert(!isNull());
        return _offset % kPageSize != 0;
    }

    bool CritPtr::isDataSpecial() const noexcept {
        assert(isData());
        return static_cast<bool>((_offset >> 31) & 1);
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

    void CritPtr::markDataSpecial() noexcept {
        assert(!isDataSpecial());
        _offset |= (1 << 31);
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
        return {_offset & (~(1 << 31))};
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

    static inline const uint16_t * smartMinElem(const uint16_t * from, const uint16_t * to) noexcept {
        if (to - from < 8) {
            return std::min_element(from, to);
        }
        __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(from));
        __m128i res = _mm_minpos_epu16(vec);
        return from + _mm_extract_epi16(res, 1);
    }

    size_t CritBitPyramid::build(const uint16_t * from, const uint16_t * to) noexcept {
        size_t size = to - from;
        if (size <= 8) {
            return smartMinElem(from, to) - from;
        }
        size_t level = 0;

        while (true) {
            const size_t q = size / 8;
            const size_t r = size % 8;
            uint16_t * val_from = _val_entry[level];
            uint8_t * idx_from = _idx_entry[level];

            for (size_t i = 0; i < q; ++i) {
                __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(from));
                __m128i res = _mm_minpos_epu16(vec);
                (*val_from++) = static_cast<uint16_t>(_mm_extract_epi16(res, 0));
                (*idx_from++) = static_cast<uint8_t>(_mm_extract_epi16(res, 1));
                from += 8;
            }
            if (r != 0) {
                const uint16_t * min_elem = std::min_element(from, to);
                (*val_from) = *min_elem;
                (*idx_from) = static_cast<uint8_t>(min_elem - from);
            }

            size = q + static_cast<size_t>(r != 0);
            if (size == 1) {
                break;
            }
            from = _val_entry[level++];
            to = from + size;
        }

        size_t idx = 0;
        size_t rank = _idx_entry[level][idx];
        const size_t lv = level;
        for (size_t i = 0; i < lv; ++i) {
            idx = idx * 8 + rank;
            rank = _idx_entry[--level][idx];
        }
        return idx * 8 + rank;
    }

    size_t CritBitPyramid::trimLeft(const uint16_t * cbegin, const uint16_t * from, const uint16_t * to) noexcept {
        int level = -1;
        size_t pos = from - cbegin;
        size_t end_pos = to - cbegin;
        assert(end_pos >= pos + 1);
        if (end_pos - pos <= 8) {
            return smartMinElem(from, to) - cbegin;
        }

        restart:
        if (end_pos - pos > 1) {
            const size_t q = pos / 8;
            const size_t r = pos % 8;

            const uint16_t * min_elem = smartMinElem(from, std::min(from + (8 - r), to));
            const size_t idx = (min_elem - from) + r;

            cbegin = _val_entry[++level];
            from = cbegin + (pos = q);
            to = cbegin + (end_pos = end_pos / 8 + static_cast<size_t>(end_pos % 8 != 0));

            *const_cast<uint16_t *>(from) = *min_elem;
            _idx_entry[level][pos] = static_cast<uint8_t>(idx);
            goto restart;
        }

        size_t idx = pos;
        // coverity[negative_returns]
        size_t rank = _idx_entry[level][pos];
        const auto lv = static_cast<size_t>(level);
        for (size_t i = 0; i < lv; ++i) {
            idx = idx * 8 + rank;
            rank = _idx_entry[--level][idx];
        }
        return idx * 8 + rank;
    }

    size_t CritBitPyramid::trimRight(const uint16_t * cbegin, const uint16_t * from, const uint16_t * to) noexcept {
        int level = -1;
        size_t pos = from - cbegin;
        size_t end_pos = to - cbegin;
        assert(end_pos >= pos + 1);
        if (end_pos - pos <= 8) {
            return smartMinElem(from, to) - cbegin;
        }

        restart:
        if (end_pos - pos > 1) {
            size_t q = end_pos / 8;
            size_t r = end_pos % 8;
            if (r == 0) {
                --q;
                r = 8;
            }

            const uint16_t * start = to - r;
            const uint16_t * min_elem = smartMinElem(std::max(from, start), to);
            const size_t idx = min_elem - start;

            cbegin = _val_entry[++level];
            from = cbegin + (pos = pos / 8);
            to = cbegin + (end_pos = q + 1);

            *const_cast<uint16_t *>(to - 1) = *min_elem;
            _idx_entry[level][end_pos - 1] = static_cast<uint8_t>(idx);
            goto restart;
        }

        size_t idx = pos;
        // coverity[negative_returns]
        size_t rank = _idx_entry[level][pos];
        const auto lv = static_cast<size_t>(level);
        for (size_t i = 0; i < lv; ++i) {
            idx = idx * 8 + rank;
            rank = _idx_entry[--level][idx];
        }
        return idx * 8 + rank;
    }

    const CritBitNode *
    parseBDNode(const BDNode * node, size_t & size, std::array<CritBitNode, kRank + 1> & array) noexcept {
        assert(!node->immut_ptrs()[0].isNull());
        if (node->immut_ptrs()[1].isNull()) {
            size = 1;
            array[0] = {UINT16_MAX, UINT16_MAX};
            return &array[0];
        }

        struct CmpObj {
            uint16_t cmp;
            uint16_t nth;
        };
        CmpObj cmp_stack_[kRank + 1];
        cmp_stack_[0].nth = kRank;
        CmpObj * const cmp_stack = cmp_stack_ + 1;

        array[0] = {UINT16_MAX, UINT16_MAX};
        cmp_stack[0] = {node->immut_diffs()[0], 0};
        CmpObj * stack_head = cmp_stack;

        for (size = 2; !(size == node->immut_ptrs().size() || node->immut_ptrs()[size].isNull()); ++size) {
            const auto i = static_cast<uint16_t>(size - 1);
            const uint16_t res_cmp = node->immut_diffs()[i];

            CmpObj * cend = stack_head + 1;
            CmpObj * h = std::lower_bound(cmp_stack, cend, CmpObj{res_cmp},
                                          [](const CmpObj & a, const CmpObj & b) noexcept {
                                              return a.cmp < b.cmp;
                                          });

            if (h != cend) { // 替换
                stack_head = h;
                array[i] = {h->nth, UINT16_MAX};
                array[(stack_head - 1)->nth].right = i;
                *stack_head = {res_cmp, i};
            } else { // 入栈
                array[i] = {UINT16_MAX, UINT16_MAX};
                array[stack_head->nth].right = i;
                *(++stack_head) = {res_cmp, i};
            }
        }
        assert(size == node->size());
        return &array[cmp_stack->nth];
    }
}