#pragma once
#ifndef LEVIDB_INDEX_FORMAT_H
#define LEVIDB_INDEX_FORMAT_H

#include <utility>

namespace levidb {
    inline bool IsKV(uint64_t rep) {

    }

    inline bool IsNode(uint64_t rep) {

    }

    inline std::pair<uint32_t, uint32_t>
    GetKVSeqAndID(uint64_t rep) {
        assert(IsKV(rep));
    };

    inline size_t GetNodeOffset(uint64_t rep) {
        assert(IsNode(rep));
    }

    uint64_t KVRep(uint32_t seq, uint32_t id) {

    }

    uint64_t NodeRep(size_t offset) {

    }
}

#endif //LEVIDB_INDEX_FORMAT_H
