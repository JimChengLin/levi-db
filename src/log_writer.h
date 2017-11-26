#pragma once
#ifndef LEVIDB8_LOG_WRITER_H
#define LEVIDB8_LOG_WRITER_H

/*
 * 将 KV 以机读 log 格式写入硬盘
 *
 * 文件格式:
 * file = [block, ...]
 * block_size = 32768(2^15)
 * block = [record, ...]
 * record = checksum + type + length + content
 * checksum = uint32_t
 * type = uint8_t
 * length = uint16_t
 *
 * type 用于拼接
 * uint8_t: [8][7][6][5][4][3][2][1]
 * [1]和[2]表示属于 batch 的哪部分
 * [1][2] = 0 || 1 || 2 || 3 = FULL || FIRST || MIDDLE || LAST
 * 只有 dependency 完整性保证, 才能接受数据, 否则丢弃并抛出异常
 * [3][4]与[1][2]基本相同, 但表示 KV 的完整性
 * [5]表示是否压缩
 * [6]表示是否删除
 * [7][8]暂不使用
 *
 * normal content 格式:
 * content = k_len + k + v
 * k_len = varint32
 *
 * compressed content 格式:
 * content = size_from_A_to_B + <A>[k_len, ...] + [v_len, ...]<B> + [k, ...] + [v, ...]
 * content 必须有序
 * size_from_A_to_B = uint16_t
 */

#include <bitset>
#include <mutex>
#include <vector>

#include "slice.h"

namespace levidb8 {
    class LogFullControlledException : public std::exception {
    };

    class AppendableFile;

    class LogWriter {
    private:
        enum ConcatType {
            FULL = 0,
            FIRST = 1,
            MIDDLE = 2,
            LAST = 3,
        };

        AppendableFile * _dst;
        size_t _block_offset = 0;
        std::mutex _emit_lock;

    public:
        explicit LogWriter(AppendableFile * dst) noexcept;

        LogWriter(AppendableFile * dst, uint64_t dst_len) noexcept;

    public:
        uint32_t addRecord(const Slice & bkv);

        uint32_t addRecordForDel(const Slice & bkv);

        uint32_t addCompressedRecords(const Slice & bkvs);

        // addrs 作为参数时表示是否 del
        std::vector<uint32_t>
        addRecordsMayDel(const std::vector<Slice> & bkvs, std::vector<uint32_t> addrs = {});

    private:
        enum Type {
            ADD_RECORD,
            ADD_RECORD_FOR_DEL,
            ADD_COMPRESSED_RECORDS,
        };

        template<Type TYPE, bool LOCK = true>
        uint32_t addRecordTpl(const Slice & bkv, ConcatType record_type = FULL);

        std::bitset<8> getCombinedType(ConcatType record_type, ConcatType kv_type,
                                       bool compress, bool del) const noexcept;

        void emitPhysicalRecord(std::bitset<8> type, const char * ptr, size_t length);

    public:
        static std::vector<uint8_t> makeRecord(const Slice & k, const Slice & v) noexcept;

        static std::vector<uint8_t> makeCompressedRecords(const std::vector<std::pair<Slice, Slice>> & kvs) noexcept;
    };
}

#endif //LEVIDB8_LOG_WRITER_H
