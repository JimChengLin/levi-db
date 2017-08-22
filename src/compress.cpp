#include <zlib.h>

#include "compress.h"

namespace LeviDB {
    namespace Compressor {
        static_assert(sizeof(Bytef) == sizeof(char), "cannot reinterpret_cast safely");

        std::vector<uint8_t> encode(const Slice & src) noexcept {
            std::vector<uint8_t> dst(compressBound(src.size()));
            size_t dst_len = dst.size();
            if (compress(dst.data(), &dst_len, reinterpret_cast<const Bytef *>(src.data()), src.size()) != Z_OK) {
                std::terminate();
            };
            dst.resize(dst_len);
            return dst;
        };

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(SimpleIterator<Slice> * src_iter) noexcept {

        };
    }
}