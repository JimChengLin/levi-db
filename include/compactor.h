#pragma once
#ifndef LEVIDB_COMPACTOR_H
#define LEVIDB_COMPACTOR_H

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

#endif //LEVIDB_COMPACTOR_H
