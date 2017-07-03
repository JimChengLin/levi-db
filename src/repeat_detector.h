#ifndef LEVIDB_REPEAT_DETECTOR_H
#define LEVIDB_REPEAT_DETECTOR_H

/*
 * 使用 GeneralSuffixTree 进行重复检测
 * 来自 PiXiu 计划, 不对 C-style 代码进行大改动
 */

#include "arena.h"
#include "skiplist.h"

namespace LeviDB {
    struct STNode {
        STNode * successor;
        STNode * parent;
        SkipList<STNode> subs;

        // 最大总长度 2**15(log record)
        uint16_t chunk_idx;
        uint16_t from;
        uint16_t to;
    };
}

#endif //LEVIDB_REPEAT_DETECTOR_H