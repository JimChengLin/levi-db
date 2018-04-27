#pragma once
#ifndef LEVIDB_INDEX_FORMAT_H
#define LEVIDB_INDEX_FORMAT_H

#include <utility>

#include "allocator.h"

namespace levidb {
    inline bool IsNode(uint64_t rep) {
        return rep >> 63;
    }

    inline bool IsKV(uint64_t rep) {
        return !IsNode(rep);
    }

    inline std::pair<uint32_t, uint32_t>
    GetKVSeqAndID(uint64_t rep) {
        assert(IsKV(rep));
        return {rep >> 32, rep & UINT32_MAX};
    };

    inline size_t GetNodeOffset(uint64_t rep) {
        assert(IsNode(rep));
        return (rep & (~(static_cast<size_t>(1) << 63))) * sgt::kPageSize;
    }

    uint64_t KVRep(uint32_t seq, uint32_t id) {
        return (static_cast<uint64_t>(seq) << 32) | id;
    }

    uint64_t NodeRep(size_t offset) {
        assert(offset % sgt::kPageSize == 0);
        return (static_cast<uint64_t>(1) << 63) | (offset / sgt::kPageSize);
    }
}

#endif //LEVIDB_INDEX_FORMAT_H
