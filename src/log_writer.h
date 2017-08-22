#ifndef LEVIDB_LOG_WRITER_H
#define LEVIDB_LOG_WRITER_H

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
 * [7][8]为 tiny checksum
 *
 * normal content 格式:
 * content = k_len + k + v
 * k_len = varint32
 *
 * compressed content 格式:
 * content = size_from_A_to_B + <A>[k_len, ...] + 0 + [v_len, ...]<B> + [k, ...] + [v, ...]
 * size_from_A_to_B = uint16_t
 */

#include <bitset>
#include <vector>

#include "env_io.h"

namespace LeviDB {
    namespace LogWriterConst {
        enum ConcatType {
            FULL = 0,
            FIRST = 1,
            MIDDLE = 2,
            LAST = 3,
        };
        static constexpr size_t block_size_ = 32768; // 2^15
        static constexpr size_t header_size_ = 4/* checksum */+ 1/* type */+ 2/* length */;
    }

    class LogWriter {
    private:
        AppendableFile * _dst;
        size_t _block_offset = 0;

    public:
        explicit LogWriter(AppendableFile * dst) noexcept : _dst(dst) {};

        LogWriter(AppendableFile * dst, uint64_t dst_len) noexcept
                : _dst(dst), _block_offset(dst_len % LogWriterConst::block_size_) {}

        ~LogWriter() noexcept = default;

        DEFAULT_MOVE(LogWriter);
        DELETE_COPY(LogWriter);

    public:
        void addRecord(const Slice & bkv) {
            addRecords({bkv}, false, false);
        };

        void addCompressRecord(const Slice & bkv) {
            addRecords({bkv}, true, false);
        };

        void addDelRecord(const Slice & bkv) {
            addRecords({bkv}, false, true);
        };

        uint32_t calcWritePos() const noexcept;

        std::vector<uint32_t> addRecords(const std::vector<Slice> & bkvs) {
            std::vector<uint32_t> addrs;
            addrs.reserve(bkvs.size());
            addRecords(bkvs, false, false, &addrs);
            return addrs;
        };

    private:
        void addRecords(const std::vector<Slice> & bkvs, bool compress, bool del,
                        std::vector<uint32_t> * addrs = nullptr);

        std::bitset<8> getCombinedType(LogWriterConst::ConcatType record_type,
                                       LogWriterConst::ConcatType kv_type,
                                       bool compress, bool del) const noexcept;

        void emitPhysicalRecord(std::bitset<8> type, const char * ptr, size_t length);

    public:
        static std::vector<uint8_t> makeRecord(const Slice & k, const Slice & v) noexcept;

        static std::vector<uint8_t> makeCompressRecord(const std::vector<std::pair<Slice, Slice>> & kvs) noexcept;
    };
}

#endif //LEVIDB_LOG_WRITER_H