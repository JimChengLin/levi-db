#pragma once
#ifndef LEVIDB8_INDEX_INTERNAL_H
#define LEVIDB8_INDEX_INTERNAL_H

#include <array>

#include "index.h"

namespace levidb8 {
    class ExpandControlledException : public std::exception {
    };

    inline bool isSpecialMask(uint8_t mask) noexcept {
        return static_cast<bool>((mask >> 7) & 1);
    }

    inline void letMaskSpecial(uint8_t & mask) noexcept {
        mask |= 1 << 7;
    }

    inline uint8_t transMask(uint8_t mask) noexcept {
        mask &= ~(1 << 7);
        assert(mask <= 7);
        return static_cast<uint8_t>(~(1 << mask));
    }

    inline uint64_t mixMarks(uint32_t diff, uint8_t mask) noexcept {
        return static_cast<uint8_t>(~(mask & ~(1 << 7))) | (diff << 8);
    }

    const uint32_t *
    unfairMinElem(const uint32_t * cbegin, const uint32_t * cend, const BDNode * node,
                  std::array<uint32_t, kRank> & calc_cache) noexcept;

    // 用于解析 OffsetToData 或 Slice
    class Matcher {
    public:
        Matcher() noexcept = default;

    public:
        virtual ~Matcher() noexcept = default;

        virtual char operator[](size_t idx) const = 0;

        virtual bool operator==(const Matcher & another) const = 0;

        virtual bool operator==(const Slice & another) const = 0;

        virtual size_t size() const = 0;

        virtual Slice toSlice() const = 0;

        // Matcher 有可能包装 multi-KV batch, target 用于区分
        virtual Slice toSlice(const Slice & target) const { return toSlice(); };
    };

    // 释放的节点写为 BDEmpty, 以链表的方式串联
    class BDEmpty {
    private:
        OffsetToEmpty _next{kDiskNull};
        std::array<uint8_t, 4> _checksum{};

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

        bool isNode() const noexcept;

        void setNull() noexcept;

        void setData(uint32_t offset) noexcept;

        void setData(OffsetToData data) noexcept;

        void setNode(uint32_t offset) noexcept;

        void setNode(OffsetToNode node) noexcept;

        OffsetToData asData() const noexcept;

        OffsetToNode asNode() const noexcept;
    };

    class BDNode {
    private:
        std::array<CritPtr, kRank + 1> _ptrs{};
        std::array<uint32_t, kRank> _diffs{};
        std::array<uint8_t, kRank> _masks{};

        uint16_t _len = 0;
        uint16_t _min_at = UINT16_MAX;
        std::array<uint8_t, 2> _padding_{};

    public:
        EXPOSE(_ptrs);

        EXPOSE(_diffs);

        EXPOSE(_masks);

        bool full() const noexcept;

        size_t size() const noexcept;

        size_t calcSize() const noexcept;

        size_t minAt() const noexcept;

        size_t calcMinAt() const noexcept;

        void setSize(uint16_t len) noexcept;

        void setMinAt(uint16_t min_at) noexcept;

        void update() noexcept;
    };

    static_assert(sizeof(BDNode) == kPageSize, "align for mmap");
    static_assert(std::is_standard_layout<BDNode>::value, "align for mmap");
}

#endif //LEVIDB8_INDEX_INTERNAL_H
