#include <zlib.h>

#include "compress.h"
#include "exception.h"

#define CHUNK 16384

namespace LeviDB {
    namespace Compressor {
        static_assert(sizeof(Bytef) == sizeof(char), "cannot reinterpret_cast safely");

        static std::string zerr(int ret) noexcept {
            std::string res;
            switch (ret) {
                case Z_ERRNO:
                    res = "error reading/writing";
                    break;
                case Z_STREAM_ERROR:
                    res = "invalid compression level";
                    break;
                case Z_DATA_ERROR:
                    res = "invalid or incomplete deflate data";
                    break;
                case Z_MEM_ERROR:
                    res = "out of memory";
                    break;
                case Z_VERSION_ERROR:
                    res = "zlib version mismatch";
                    break;
                default:
                    res = std::to_string(ret);
                    break;
            }
            return res;
        }

        std::vector<uint8_t> encode(const Slice & src) noexcept {
            std::vector<uint8_t> dst(compressBound(src.size()));
            size_t dst_len = dst.size();
            if (compress(dst.data(), &dst_len, reinterpret_cast<const Bytef *>(src.data()), src.size()) != Z_OK) {
                std::terminate();
            };
            dst.resize(dst_len);
            return dst;
        };

        class DecodeIterator : public SimpleIterator<Slice> {
        private:
            Slice _item;
            std::unique_ptr<SimpleIterator<Slice>> _src_iter;

            z_stream strm{};
            int ret{};
            int _line = 0;
            unsigned char out[CHUNK]{};

        public:
            explicit DecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter)
                    : _src_iter(std::move(src_iter)) {
                static_assert(Z_NULL == 0, "fail then strm is not initialized");
                inflateInit(&strm);
                next();
            }

            DELETE_MOVE(DecodeIterator);
            DELETE_COPY(DecodeIterator);

            ~DecodeIterator() noexcept override { inflateEnd(&strm); };

            bool valid() const override { return _line != -1; };

            Slice item() const override { return _item; };

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

            void next() override {
                GEN_INIT();
                    do {
                        if (!_src_iter->valid()) {
                            throw Exception::corruptionException("zlib", zerr(Z_ERRNO));
                        }
                        strm.next_in = const_cast<Bytef *>(reinterpret_cast<const Bytef *>(_src_iter->item().data()));
                        strm.avail_in = static_cast<uInt>(_src_iter->item().size());
                        assert(strm.avail_in > 0);

                        do {
                            strm.next_out = out;
                            strm.avail_out = CHUNK;
                            ret = inflate(&strm, Z_NO_FLUSH);
                            assert(ret != Z_STREAM_ERROR);
                            switch (ret) {
                                case Z_NEED_DICT:
                                    ret = Z_DATA_ERROR;
                                case Z_DATA_ERROR:
                                case Z_MEM_ERROR:
                                    throw Exception::corruptionException("zlib", zerr(ret));
                                default:;
                            }
                            _item = Slice(out, CHUNK - strm.avail_out);
                            YIELD();
                        } while (strm.avail_out == 0);

                        if (ret != Z_STREAM_END) {
                            _src_iter->next();
                        } else {
                            break;
                        }
                    } while (true);
                GEN_STOP();
            };
        };

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter) {
            return std::make_unique<DecodeIterator>(std::move(src_iter));
        };
    }
}