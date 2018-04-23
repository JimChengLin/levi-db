#pragma once
#ifndef LEVIDB_COMPACTER_H
#define LEVIDB_COMPACTER_H

/*
 * Compaction 过滤接口
 */

#include "slice.h"

namespace levidb {
    class Compactor {
    public:
        Compactor() = default;

        virtual ~Compactor() = default;

    public:
        virtual bool ShouldDrop(const Slice & k) const {
            return false;
        }
    };
}

#endif //LEVIDB_COMPACTER_H
