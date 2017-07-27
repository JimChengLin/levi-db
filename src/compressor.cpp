#include "coder.h"
#include "coding.h"
#include "compressor.h"
#include "crc32c.h"

#ifndef __clang__
#include <algorithm> // GCC
#endif

namespace LeviDB {
    void Compressor::submitDel(const Slice & key) noexcept {
        assert(!valid());

        char buf[5];
        char * end = Coding::encodeVarint32(buf, static_cast<uint32_t>(key.size() + 1/* deletion mark */));
        size_t len = end - buf;
        _spec_varint_len = static_cast<int>(len);

        _src.reset(new char[len + key.size()]);
        char * src = _src.get();
        _src_begin = src;

        memcpy(src, buf, len);
        src += len;
        memcpy(src, key.data(), key.size());
        src += key.size();
        _src_end = src;
    }

    void Compressor::submit(const Slice & key, const Slice & val) noexcept {
        assert(!valid());

        char buf[5];
        char * end = Coding::encodeVarint32(buf, static_cast<uint32_t>(key.size()));
        size_t len = end - buf;
        _spec_varint_len = static_cast<int>(len);

        _src.reset(new char[len + key.size() + val.size()]);
        char * src = _src.get();
        _src_begin = src;

        memcpy(src, buf, len);
        src += len;
        memcpy(src, key.data(), key.size());
        src += key.size();
        memcpy(src, val.data(), val.size());
        src += val.size();
        _src_end = src;
    }

#define checksum_len sizeof(uint32_t)

    std::pair<std::vector<uint8_t>, CompressorConst::CompressType>
    Compressor::next(uint16_t n, bool compress, uint32_t cursor) noexcept {
        assert(valid());
        assert(n > checksum_len && n <= 32768/* 2^15 */);

        std::vector<uint8_t> res;
        size_t length = std::min<size_t>(_src_end - _src_begin, n);

        auto compress_type = CompressorConst::NO_COMPRESS;
        char * next_begin = _src_begin;
        if (compress && length > checksum_len) {
            compress_type = CompressorConst::CODER_COMPRESS;
            next_begin = _src_begin + length;

            int skip = 0;
            size_t arr_len = length;
            std::vector<int> opt_head = maySpecCmd(cursor);
            if (!opt_head.empty()) {
                skip = _spec_varint_len + _spec_len;
                cursor += skip;
                arr_len -= skip;
            }

            std::vector<int> codes;
            if (arr_len != 0) {
                char * arr = _arena.allocate(arr_len);
                memcpy(arr, _src_begin + skip, arr_len);
                codes = _tree.setitem(Slice(arr, arr_len));
                _compressed_bytes += arr_len;
                _anchors.emplace_back(cursor);
            }
            res = process(codes, cursor, compress_type/* may change */, std::move(opt_head), skip);
        }

        if (res.empty() || res.size() >= n ||
            (compress_type != CompressorConst::NO_COMPRESS && res.size() > (length + checksum_len) * 7 / 8)) {
            compress_type = CompressorConst::NO_COMPRESS;
            length = std::min<size_t>(length + checksum_len, n);
            res.resize(length);

            length -= checksum_len;
            uint32_t checksum = CRC32C::value(_src_begin, length);
            memcpy(&res[0], _src_begin, length);
            memcpy(&res[length], &checksum, checksum_len);
            next_begin = _src_begin + length;
        }
        assert(next_begin > _src_begin);
        _src_begin = next_begin;
        assert(res.size() <= n);
        assert(_src_begin <= _src_end);

        emitSpecCmd(); // no arg = reset
        if (_compressed_bytes > CompressorConst::reset_threshold) { reset(); }
        return {std::move(res), compress_type};
    }

    void Compressor::reset() noexcept {
        if (!valid()) {
            _src_begin = nullptr;
            _src_end = nullptr;
            _src = nullptr;
        }

        _arena.reset();
        _anchors.clear();
        _compressed_bytes = 0;
        // reset
        _tree.~SuffixTree();
        new(&_tree) SuffixTree(&_arena);
    }

    std::vector<uint8_t> Compressor::process(const std::vector<int> & codes, uint32_t cursor,
                                             CompressorConst::CompressType & flagMayChange,
                                             std::vector<int> && opt_head, int skip) noexcept {
        assert(valid() && flagMayChange == CompressorConst::CODER_COMPRESS);
        assert(_anchors.size() == _tree._chunk.size());

        std::vector<int> codes_ir(std::move(opt_head));
        codes_ir.reserve(codes_ir.size() + codes.size());

        for (int i = 0; i < codes.size(); ++i) {
            int symbol = codes[i];
            if (symbol != CoderConst::FN) {
                assert(symbol <= UINT8_MAX);
                codes_ir.push_back(symbol);
            } else {
                assert(symbol <= UINT16_MAX);
                int chunk_idx = codes[++i];
                int from = codes[++i];
                int to = codes[++i];

                uint32_t chunk_offset = cursor - _anchors[chunk_idx];
                if (chunk_offset > UINT16_MAX) {
                    codes_ir.insert(codes_ir.end(),
                                    reinterpret_cast<const uint8_t *>(_tree._chunk[chunk_idx].data() + from),
                                    reinterpret_cast<const uint8_t *>(_tree._chunk[chunk_idx].data() + to));
                    continue;
                }
                if (chunk_offset == 0) { from += skip; }
                int len = to - from;

                uint8_t buf[7]; // uint8_t(mask) + uint16_t * 3
                uint8_t * p = buf;
                Compressor::appendCompressInfo(p, chunk_offset, from, len);

                if (1/* FN */+ (p - buf) < len) {
                    codes_ir.emplace_back(CoderConst::FN);
                    codes_ir.insert(codes_ir.end(), buf, p);
                } else {
                    codes_ir.insert(codes_ir.end(),
                                    reinterpret_cast<const uint8_t *>(_tree._chunk[chunk_idx].data() + from),
                                    reinterpret_cast<const uint8_t *>(_tree._chunk[chunk_idx].data() + to));
                }
            }
        }

        Coder coder;
        std::vector<uint8_t> res = coder.encode(codes_ir);
        if (res.size() >= (codes_ir.size() + checksum_len) // SimpleCode
            && std::find(codes_ir.cbegin(), codes_ir.cend(), CompressorConst::unique) == codes_ir.cend()) {
            res.resize(codes_ir.size() + checksum_len);
            flagMayChange = CompressorConst::SIMPLE_COMPRESS;

            std::replace_copy(codes_ir.cbegin(), codes_ir.cend(), res.begin(),
                              static_cast<int>(CoderConst::FN),
                              static_cast<int>(CompressorConst::unique));
            uint32_t checksum = CRC32C::value(reinterpret_cast<char *>(res.data()), codes_ir.size());
            memcpy(&res[codes_ir.size()], &checksum, checksum_len);

            auto cend = res.cend() - checksum_len;
            if (std::find(res.cbegin(), cend, CompressorConst::unique) == cend) {
                flagMayChange = CompressorConst::NO_COMPRESS;
            }
        }
        return res;
    }

    std::vector<int> Compressor::maySpecCmd(uint32_t cursor) const noexcept {
        if (_spec_len != -1) {
            uint8_t buf[7];
            uint8_t * p = buf;
            Compressor::appendCompressInfo(p, cursor - _spec_anchor, _spec_from, _spec_len);

            if (1 + (p - buf) < _spec_len) {
                std::vector<int> res;
                res.insert(res.end(),
                           reinterpret_cast<const uint8_t *>(_src_begin),
                           reinterpret_cast<const uint8_t *>(_src_begin + _spec_varint_len));
                res.emplace_back(CoderConst::FN);
                res.insert(res.end(), buf, p);
                return res;
            }
        }
        return {};
    }

    void Compressor::appendCompressInfo(uint8_t *& p, int chunk_offset, int from, int len) noexcept { // static method
        uint8_t mark = 0;
        for (int val:{chunk_offset, from, len}) {
            mark <<= 1;
            if (val <= UINT8_MAX) {
            } else {
                assert(val <= UINT16_MAX);
                mark |= 1;
            }
        }
        if (mark != 0) {
            *(p++) = mark;
        }

        for (int val:{chunk_offset, from, len}) {
            if (val <= UINT8_MAX) {
                *(p++) = val;
            } else {
                auto tmp = static_cast<uint16_t>(val);
                memcpy(p, &tmp, sizeof(tmp));
                p += sizeof(tmp);
            }
        }
    }
}