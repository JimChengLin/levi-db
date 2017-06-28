#ifndef LEVIDB_LOG_FORMAT_H
#define LEVIDB_LOG_FORMAT_H

/*
 * 机读 log 设定
 *
 * 文件格式:
 * file = [block ...]
 * block_size = 32768 (2^15)
 * block = [record ...]
 * record = type + length + content
 * type = uint8_t
 * length = uint16_t
 * length + content 足以还原出部分的数据
 *
 * type 用于拼接
 * uint8_t: [1][2][3][4][5][6][7][8]
 * [1]表示是否是 batch 的一部分, 是则为1, [2]和[3]才有意义
 * [2][3] = 0 || 1 || 2 || 3, FULL, FIRST, MIDDLE, LAST
 * 只有 batch 的完整性保证, 才能接受数据, 否则丢弃并报告异常
 * [4][5]与[2][3]基本相同, 但表示 KV 的完整性且永远有值
 * [6]表示是否压缩
 * [7][8]为迷你 checksum
 */

namespace LeviDB {
    namespace log {
        enum ConcatType {
            FULL = 0,

            FIRST = 1,
            MIDDLE = 2,
            LAST = 3,
        };
    }
}

#endif //LEVIDB_LOG_FORMAT_H