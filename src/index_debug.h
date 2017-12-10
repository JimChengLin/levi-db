#pragma once
#ifndef LEVIDB8_MATCHER_DEBUG_H
#define LEVIDB8_MATCHER_DEBUG_H

#include "index.h"
#include "index.hpp"
#include "index_scan.hpp"

namespace levidb8 {
    class MatcherDebugOffset;

    class MatcherDebugSlice;

    class CacheDebug {
    public:
        explicit CacheDebug() noexcept = default;
    };

    class MatcherDebugOffset {
    private:
        uint32_t _val;

    public:
        MatcherDebugOffset(OffsetToData data, CacheDebug &) noexcept : _val(data.val) {}

        bool operator==(const Slice & another) const noexcept {
            return toSlice() == another;
        }

        Slice toSlice() const noexcept {
            return Slice(reinterpret_cast<const char *>(&_val), sizeof(_val));
        }

        Slice toSlice(const USR &) const noexcept {
            return toSlice();
        }

        bool isCompress() const noexcept {
            return false;
        }
    };

    class MatcherDebugSlice {
    private:
        Slice _slice;

    public:
        explicit MatcherDebugSlice(Slice slice) noexcept : _slice(std::move(slice)) {}

        char operator[](size_t idx) const noexcept {
            return _slice[idx];
        }

        size_t size() const noexcept {
            return _slice.size();
        }
    };

    using BitDegradeTreeDebug = BitDegradeTree<MatcherDebugOffset, MatcherDebugSlice, CacheDebug>;
}

#endif //LEVIDB8_MATCHER_DEBUG_H
