#ifndef LEVIDB_COMPRESS_H
#define LEVIDB_COMPRESS_H

/*
 * 对 zlib 的简单封装
 * 可更换为其它压缩库
 */

#include <vector>

#ifndef __clang__
#include <memory>
#endif

#include "iterator.h"
#include "slice.h"

namespace LeviDB {
    namespace Compressor {
        std::vector<uint8_t> encode(const Slice & src) noexcept;

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter);

        uint32_t decoderPosition(SimpleIterator<Slice> * decoder) noexcept;
    }
}

#endif //LEVIDB_COMPRESS_H