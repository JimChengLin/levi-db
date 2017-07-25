#ifndef LEVIDB_LOG_WRITER_H
#define LEVIDB_LOG_WRITER_H

/*
 * 将 KV 写入硬盘
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

#include "compressor.h"
#include "env_io.h"

namespace LeviDB {
    namespace LogWriterConst {
        enum ConcatType {
            FULL = 0,
            FIRST = 1,
            MIDDLE = 2,
            LAST = 3,
        };
        static constexpr int block_size = 32768; // 2^15
        static constexpr int header_size = 1/* type */+ 2/* length */;
        static constexpr int min_size = header_size + sizeof(uint32_t)/* for checksum*/;
    }

    class LogWriter {
    private:
        AppendableFile * _dst;
        Compressor _compressor;
        int _block_offset;

    public:
        typedef std::tuple<Slice/* key */, Slice/* val */, bool/* del */> Record;

        explicit LogWriter(AppendableFile * dst) noexcept : _dst(dst), _block_offset(0) {};

        LogWriter(AppendableFile * dst, uint64_t dst_len) noexcept
                : _dst(dst), _block_offset(static_cast<int>(dst_len % LogWriterConst::block_size)) {}

        ~LogWriter() noexcept = default;

        void addRecord(const Record & slice, bool compress = true) { return addRecords({slice}, compress); }

        void addRecords(const std::vector<Record> & records, bool compress = true);

        // 禁止复制
        LogWriter(const LogWriter &) = delete;

        void operator=(const LogWriter &) = delete;

    private:
        void emitPhysicalRecord(uint8_t type, const Slice & data);

        static uint8_t getCombinedType(LogWriterConst::ConcatType record_type,
                                       LogWriterConst::ConcatType kv_type,
                                       CompressorConst::CompressType compress_type) noexcept;
    };
}

#endif //LEVIDB_LOG_WRITER_H