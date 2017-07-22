#ifndef LEVIDB_COMPRESSOR_H
#define LEVIDB_COMPRESSOR_H

/*
 * 压缩器(compressor)按执行顺序由两部分组成,
 * 1. 重复字符串检测器(repeat_detector)
 * 2. 算术编码器(coder) / SimpleCode(do nothing)
 *
 * 长度缩短12.5%以上才接受压缩后的编码,
 * 否则返回 (原编码 || SimpleCode) + checksum
 *
 * 调用步骤:
 * 提交源字符串, 再不断申请片段,
 * 必须回填上段实际写入位置
 *
 * 内部格式:
 * KV 会被转换为
 * content = varint32 k_length + key + val
 * del 时, k_length += 1
 */

#include "arena.h"
#include "repeat_detector.h"
#include "slice.h"
#include <memory>
#include <vector>

namespace LeviDB {
    namespace CompressorConst {
        static constexpr int reset_threshold = UINT16_MAX;
        static constexpr uint8_t unique = 251; // for SimpleCode

        enum CompressType {
            NO_COMPRESS,
            CODER_COMPRESS,
            SIMPLE_COMPRESS,
        };

        enum ImpossibleOffset { // min offset is 8 because of the log format
            U8U8U16 = 0b001, // don't need U8U8U8(0b000)
            U8U16U8 = 0b010,
            U8U16U16 = 0b011,
//            U16U8U8   = 0b100,
//            U16U8U16  = 0b101,
//            U16U16U8  = 0b110,
//            U16U16U16 = 0b111,
        };
    }

    class Compressor {
    private:
        char * _src_begin = nullptr;
        char * _src_end = nullptr;
        std::unique_ptr<char[]> _src;

        Arena _arena;
        SuffixTree _tree;
        std::vector<uint32_t> _anchors;
        int _compressed_bytes = 0;

        // 特殊压缩指令
        int _spec_chunk_offset = -1;
        int _spec_from = -1;
        int _spec_len = -1;
        int _spec_varint_len = -1;

    public:
        Compressor() noexcept : _src(nullptr), _tree(&_arena) {};

        ~Compressor() noexcept = default;

        void submit(const Slice & key, const Slice & val = Slice()) noexcept;

        void submitDel(const Slice & key) noexcept;

        // 单次最大长度 2^15(max log record length)
        std::pair<std::vector<uint8_t>, CompressorConst::CompressType>
        nextCompressed(uint16_t n, int cursor) noexcept { return next(n, true, cursor); }

        std::pair<std::vector<uint8_t>, CompressorConst::CompressType>
        nextUncompressed(uint16_t n) noexcept { return next(n, false, -1); }

        bool valid() const noexcept { return _src_begin != _src_end; };

        void reset() noexcept;

        void emitSpecCmd(int chunk_offset = -1, int from = -1, int len = -1, int varint_len = -1) noexcept {
            _spec_chunk_offset = chunk_offset;
            _spec_from = from;
            _spec_len = len;
            _spec_varint_len = varint_len;
        }

        // 禁止复制
        Compressor(const Compressor &) = delete;

        void operator=(const Compressor &) = delete;

    private:
        std::pair<std::vector<uint8_t>, CompressorConst::CompressType>
        next(uint16_t n, bool compress, int cursor) noexcept;

        std::vector<uint8_t> process(const std::vector<int> & codes, int cursor,
                                     CompressorConst::CompressType & flagMayChange,
                                     std::vector<int> && opt_head, int skip) noexcept;

        std::vector<int> maySpecCmd() const noexcept;

        static void appendCompressInfo(char *& p, int chunk_offset, int from, int len) noexcept;
    };
}

#endif //LEVIDB_COMPRESSOR_H