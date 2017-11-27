#include <zstd.h>

#include "compress.h"
#include "config.h"
#include "exception.h"

namespace levidb8 {
    namespace compressor {
        std::vector<uint8_t> encode(const Slice & src) {
            std::vector<uint8_t> dst(ZSTD_compressBound(src.size()));
            size_t dst_len = ZSTD_compress(dst.data(), dst.size(), src.data(), src.size(), kZstdLevel);
            if (static_cast<bool>(ZSTD_isError(dst_len))) {
                throw Exception::corruptionException("error compressing: ", ZSTD_getErrorName(dst_len));
            }
            dst.resize(dst_len);
            return dst;
        }

        class DecodeIterator : public SimpleIterator<Slice> {
        private:
            std::unique_ptr<SimpleIterator<Slice>> _src_iter;
            Slice _item;
            int _line = 0;

            std::string _buff_out;
            size_t _threshold{};

            struct DSD {
                void operator()(ZSTD_DStream * dstream) noexcept {
                    ZSTD_freeDStream(dstream);
                }
            };

            std::unique_ptr<ZSTD_DStream, DSD> _dstream;
            ZSTD_inBuffer _input{};

        public:
            explicit DecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter) noexcept
                    : _src_iter(std::move(src_iter)) {}

            ~DecodeIterator() noexcept override = default;

            bool valid() const override {
                return _line != -1;
            }

            void prepare() override {
                if (_line == 0) {
                    _src_iter->prepare();
                    _buff_out.resize(ZSTD_DStreamOutSize());
                    _threshold = _buff_out.size() / 3;

                    _dstream.reset(ZSTD_createDStream());
                    if (_dstream == nullptr) {
                        throw Exception::corruptionException("ZSTD_createDStream() error");
                    }
                    size_t r = ZSTD_initDStream(_dstream.get());
                    if (static_cast<bool>(ZSTD_isError(r))) {
                        throw Exception::corruptionException("ZSTD_initDStream() error:", ZSTD_getErrorName(r));
                    }
                }
            }

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

            void next() override {
                assert(valid());
                GEN_INIT();
                    for (; _src_iter->valid();
                           _src_iter->next()) {
                        {
                            Slice s = _src_iter->item();
                            _input = {s.data(), s.size(), 0};
                        }
                        _item = {};
                        while (_input.pos < _input.size) {
                            ZSTD_outBuffer output = {&_buff_out[_item.size()], _buff_out.size() - _item.size(), 0};
                            if (output.size < _threshold) {
                                _buff_out.resize(_buff_out.capacity() + 1);
                                _buff_out.resize(_buff_out.capacity());
                            }

                            size_t r = ZSTD_decompressStream(_dstream.get(), &output, &_input);
                            if (static_cast<bool>(ZSTD_isError(r))) {
                                throw Exception::corruptionException("ZSTD_decompressStream() error:",
                                                                     ZSTD_getErrorName(r));
                            }
                            _item = {_buff_out.data(), _item.size() + output.pos};
                        }
                        YIELD();
                    }
                GEN_STOP();
            }

            Slice item() const override {
                assert(valid());
                return _item;
            }
        };

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter) noexcept {
            return std::make_unique<DecodeIterator>(std::move(src_iter));
        }
    }
}