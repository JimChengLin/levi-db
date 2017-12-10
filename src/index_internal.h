#pragma once
#ifndef LEVIDB8_INDEX_INTERNAL_H
#define LEVIDB8_INDEX_INTERNAL_H

#include <array>

#include "index.h"

namespace levidb8 {
    static constexpr OffsetToNode _root{0};

    class ExpandControlledException : public std::exception {
    };

    inline uint16_t getDiffAt(uint16_t diff) noexcept {
        return diff >> 3;
    }

    // shift ↑ mask ↓
    inline uint8_t getShift(uint16_t diff) noexcept {
        return static_cast<uint8_t>(~diff & 0b111);
    }

    class BDEmpty {
    private:
        OffsetToEmpty _next{kDiskNull};
        uint32_t _checksum{};

    public:
        EXPOSE(_next);

        bool verify() const noexcept;

        void updateChecksum() noexcept;
    };

    class CritPtr {
    private:
        // 被 kPageSize 整除表示 OffsetToNode
        // 反之表示 OffsetToData
        uint32_t _offset = kDiskNull;

    public:
        bool isNull() const noexcept;

        bool isData() const noexcept;

        bool isDataSpecial() const noexcept;

        bool isNode() const noexcept;

        void setNull() noexcept;

        void setData(uint32_t offset) noexcept;

        void setData(OffsetToData data) noexcept;

        void markDataSpecial() noexcept;

        void setNode(uint32_t offset) noexcept;

        void setNode(OffsetToNode node) noexcept;

        OffsetToData asData() const noexcept;

        OffsetToNode asNode() const noexcept;
    };

    class BDNode {
    private:
        std::array<CritPtr, kRank + 1> _ptrs{};
        std::array<uint16_t, kRank> _diffs;

    public:
        EXPOSE(_ptrs);

        EXPOSE(_diffs);

        bool full() const noexcept;

        size_t size() const noexcept;
    };

    static_assert(sizeof(BDNode) == kPageSize, "align for mmap");
    static_assert(std::is_standard_layout<BDNode>::value, "align for mmap");

    class CritBitPyramid {
    private:
        uint16_t _val_1[86];
        uint16_t _val_2[11];
        uint16_t _val_3[2];
        uint16_t _val_4[1];
        uint8_t _idx_1[86];
        uint8_t _idx_2[11];
        uint8_t _idx_3[2];
        uint8_t _idx_4[1];
        uint16_t * _val_entry[4] = {_val_1, _val_2, _val_3, _val_4};
        uint8_t * _idx_entry[4] = {_idx_1, _idx_2, _idx_3, _idx_4};

    public:
        size_t build(const uint16_t * from, const uint16_t * to) noexcept;

        size_t trimLeft(const uint16_t * cbegin, const uint16_t * from, const uint16_t * to) noexcept;

        size_t trimRight(const uint16_t * cbegin, const uint16_t * from, const uint16_t * to) noexcept;
    };

    struct CritBitNode {
        uint16_t left;
        uint16_t right;
    };

    const CritBitNode *
    parseBDNode(const BDNode * node, size_t & size, std::array<CritBitNode, kRank + 1> & array) noexcept;
}

#endif //LEVIDB8_INDEX_INTERNAL_H
