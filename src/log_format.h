#ifndef LEVIDB_LOG_FORMAT_H
#define LEVIDB_LOG_FORMAT_H

/*
 * 机读 log 设定
 *
 * 文件格式:
 * file = [block ...]
 * block_size = 32768(2^15)
 * block = [record ...]
 * record = type + length + content
 * type = uint8_t
 * length = uint16_t
 *
 * type 用于拼接
 * uint8_t: [1][2][3][4][5][6][7][8]
 * [1]和[2]表示属于 batch 的哪部分
 * [1][2] = 0 || 1 || 2 || 3 = FULL || FIRST || MIDDLE || LAST
 * 只有 batch 的完整性保证, 才能接受数据, 否则丢弃并报告异常
 * [3][4]与[1][2]基本相同, 但表示 KV 的完整性
 * [5]表示是否压缩
 * [6]表示压缩类型: 算术编码器 || SimpleCode
 * [7][8]为 tiny checksum
 */

namespace LeviDB {
    namespace LogConst {
        enum ConcatType {
            FULL = 0,
            FIRST = 1,
            MIDDLE = 2,
            LAST = 3,
        };
    }
}

#endif //LEVIDB_LOG_FORMAT_H