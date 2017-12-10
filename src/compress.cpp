#include "../include/exception.h"
#include "compress.h"

namespace levidb8 {
    namespace compress {
        std::vector<uint8_t> encode(const Slice & src) {
            std::vector<uint8_t> dst(ZSTD_compressBound(src.size()));
            size_t dst_len = ZSTD_compress(dst.data(), dst.size(), src.data(), src.size(), kZstdLevel);
            if (static_cast<bool>(ZSTD_isError(dst_len))) {
                throw Exception::corruptionException("error compressing: ", ZSTD_getErrorName(dst_len));
            }
            dst.resize(dst_len);
            return dst;
        }

        Decoder::Decoder(std::unique_ptr<SimpleIterator<Slice>> && src)
                : _src(std::move(src)),
                  _dstream(ZSTD_createDStream()) {
            if (_dstream == nullptr) {
                throw Exception::corruptionException("ZSTD_createDStream() error");
            }
            size_t r = ZSTD_initDStream(_dstream.get());
            if (static_cast<bool>(ZSTD_isError(r))) {
                throw Exception::corruptionException("ZSTD_initDStream() error:", ZSTD_getErrorName(r));
            }
        }

        bool Decoder::valid() const noexcept {
            return _line > 0;
        }

        Slice Decoder::item() const noexcept {
            assert(valid());
            return _item;
        }

        bool Decoder::isLastPage() const noexcept {
            assert(valid());
            return _lastPage;
        }

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

        void Decoder::next() {
            GEN_INIT();
                while (true) {
                    _src->next();
                    if (!_src->valid()) {
                        break;
                    }
                    {
                        Slice s = _src->item();
                        _input = {s.data(), s.size(), 0};
                    }
                    _lastPage = false;
                    while (true) {
                        {
                            ZSTD_outBuffer output = {_buff_out, sizeof(_buff_out), 0};
                            size_t r = ZSTD_decompressStream(_dstream.get(), &output, &_input);
                            if (static_cast<bool>(ZSTD_isError(r))) {
                                throw Exception::corruptionException("ZSTD_decompressStream() error:",
                                                                     ZSTD_getErrorName(r));
                            }
                            _item = {_buff_out, output.pos};
                        }
                        if (_input.pos < _input.size) {
                            YIELD();
                        } else {
                            _lastPage = true;
                            YIELD()
                            break;
                        }
                    }
                }
            GEN_STOP();
        }

        void Decoder::reset() {
            _line = 0;
            size_t r = ZSTD_initDStream(_dstream.get());
            if (static_cast<bool>(ZSTD_isError(r))) {
                throw Exception::corruptionException("ZSTD_initDStream() error:", ZSTD_getErrorName(r));
            }
        }
    }
}