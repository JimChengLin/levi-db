#include <zlib.h>

#include "compress.h"
#include "exception.h"

namespace LeviDB {
    namespace Compressor {
        static_assert(sizeof(Bytef) == sizeof(char), "cannot reinterpret_cast safely");

        std::vector<uint8_t> encode(const Slice & src) noexcept {
            std::vector<uint8_t> dst(compressBound(src.size()));
            size_t dst_len = dst.size();
            if (compress(dst.data(), &dst_len, reinterpret_cast<const Bytef *>(src.data()), src.size()) != Z_OK) {
                std::terminate();
            };

            uint64_t uncompress_len = src.size();
            dst.resize(dst_len + sizeof(uncompress_len));
            memcpy(&dst[dst_len], &uncompress_len, sizeof(uncompress_len));
            return dst;
        };

        std::vector<uint8_t> decode(const Slice & src) {
            uint64_t uncompress_len;
            memcpy(&uncompress_len, src.data() + src.size() - sizeof(uncompress_len), sizeof(uncompress_len));
            size_t dst_len = uncompress_len;

            std::vector<uint8_t> dst(dst_len);
            int ret = uncompress(dst.data(), &dst_len, reinterpret_cast<const Bytef *>(src.data()), src.size());
            if (ret != Z_OK) {
                throw Exception::corruptionException("zlib error code: " + std::to_string(ret));
            };
            assert(dst_len == uncompress_len);
            return dst;
        };

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(SimpleIterator<Slice> * src_iter) noexcept {

        };
    }
}