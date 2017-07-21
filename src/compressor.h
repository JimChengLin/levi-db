#ifndef LEVIDB_COMPRESSOR_H
#define LEVIDB_COMPRESSOR_H

/*
 * 压缩器(compressor)按执行顺序由两部分组成,
 * 1. 重复字符串检测器(repeat_detector)
 * 2. 算术编码器(coder)
 *
 * 长度缩短12.5%以上才接受压缩后的编码,
 * 否则返回 原编码 + checksum
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
#include <tuple>
#include <vector>

namespace LeviDB {
    namespace CompressorConst {
        static constexpr int reset_threshold = UINT16_MAX;

//        enum ImpossibleOffset { // min offset is 8 because of the log format
//            U8U8U16   = 0b001, // don't need U8U8U8(0b000)
//            U8U16U8   = 0b010,
//            U8U16U16  = 0b011,
//            U16U8U8   = 0b100,
//            U16U8U16  = 0b101,
//            U16U16U8  = 0b110,
//            U16U16U16 = 0b111,
//        };
    }

    class Compressor {
    private:
        char * _src_begin;
        char * _src_end;
        std::unique_ptr<char[]> _src;

        Arena _arena;
        SuffixTree _tree;
        std::vector<uint32_t> _anchors;
        int _compressed_bytes;

    public:
        Compressor() noexcept
                : _src_begin(nullptr), _src_end(nullptr), _src(nullptr),
                  _arena(), _tree(&_arena), _anchors(), _compressed_bytes(0) {};

        ~Compressor() noexcept {}

        void submit(const Slice & key, const Slice & val, bool del = false) noexcept;

        // 单次最大长度 2^15(max log record length)
        std::tuple<std::vector<uint8_t>/* result */, bool/* compress */, bool/* feedback */>
        next(uint16_t n, bool compress, int cursor = -1) noexcept;

        bool valid() const noexcept { return _src_begin != _src_end; };

        void feedback(uint32_t anchor) noexcept;

        void reset() noexcept;

    private:
        std::vector<uint8_t> process(std::vector<int> & codes, int cursor) noexcept;

        // 禁止复制
        Compressor(const Compressor &);

        void operator=(const Compressor &);
    };
}

#endif //LEVIDB_COMPRESSOR_H