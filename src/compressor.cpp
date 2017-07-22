#include "coder.h"
#include "coding.h"
#include "compressor.h"
#include "crc32c.h"

namespace LeviDB {
    void Compressor::submitDel(const Slice & key) noexcept {
        assert(!valid());

        char buf[5];
        char * end = Coding::encodeVarint32(buf, static_cast<uint32_t>(key.size() + 1));
        size_t len = end - buf;

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

    std::pair<std::vector<uint8_t>/* result */, bool/* compress */>
    Compressor::next(uint16_t n, bool compress, int cursor) noexcept {
        assert(valid());
        assert(n > checksum_len && n <= 32768/* 2^15 */);

        std::vector<uint8_t> res;
        size_t length = std::min<size_t>(_src_end - _src_begin, n);

        bool compressed = false;
        bool require_feedback = false;
        char * next_begin = _src_begin;
        if (compress) {
            assert(cursor >= 0);

            compressed = true;
            require_feedback = true;
            next_begin = _src_begin + length;

            char * arr = _arena.allocate(length);
            memcpy(arr, _src_begin, length);
            std::vector<int> codes = _tree.setitem(Slice(arr, length));
            _compressed_bytes += length;

            res = process(codes, cursor);
        }

        if (!compress || res.size() > (length + checksum_len) * 7 / 8) {
            compressed = false;

            length = std::min<size_t>(length + checksum_len, n);
            res.resize(length);

            length -= checksum_len;
            uint32_t checksum = CRC32C::value(_src_begin, length);
            memcpy(&res[0], &checksum, checksum_len);
            memcpy(&res[checksum_len], _src_begin, length);
            next_begin = _src_begin + length;
        }
        _src_begin = next_begin;
        assert(res.size() <= n);

        if (_compressed_bytes > CompressorConst::reset_threshold) { reset(); }
        else if (require_feedback) { _anchors.emplace_back(cursor); }
        return {std::move(res), compressed};
    }

    void Compressor::reset() noexcept {
        assert(!valid());
        _src_begin = nullptr;
        _src_end = nullptr;
        _src = nullptr;

        _arena.reset();
        _anchors.clear();
        _compressed_bytes = 0;

        // reset
        _tree.~SuffixTree();
        new(&_tree) SuffixTree(&_arena);
    }

    std::vector<uint8_t> Compressor::process(std::vector<int> & codes, int cursor) noexcept {
        assert(valid());
        assert(_anchors.size() + 1 == _tree._chunk.size());

        auto append_mark_dat = [](char *& p, int chunk_offset, int from, int len) {
            char mark = 0;
            for (int val:{chunk_offset, from, len}) {
                if (val <= UINT8_MAX) {
                } else {
                    assert(val <= UINT16_MAX);
                    mark |= 1;
                }
                mark <<= 1;
            }
            if (mark != 0) {
                *(p++) = mark;
            }
            // dat
            for (int val:{chunk_offset, from, len}) {
                if (val <= UINT8_MAX) {
                    *(p++) = val;
                } else {
                    assert(val <= UINT16_MAX);
                    auto tmp = static_cast<uint16_t>(val);
                    memcpy(p, &tmp, sizeof(tmp));
                    p += sizeof(tmp);
                }
            }
        };

        std::vector<int> codes_ir;
        codes_ir.reserve(codes.size());

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

                int chunk_offset = cursor - _anchors[chunk_idx];
                if (chunk_offset > UINT16_MAX) {
                    codes_ir.insert(codes_ir.end(),
                                    _tree._chunk[chunk_idx].data() + from,
                                    _tree._chunk[chunk_idx].data() + to);
                    continue;
                }
                int len = to - from;

                char buf[7]; // uint8_t(mask) + uint16_t * 3
                char * p = buf;
                append_mark_dat(p, chunk_offset, from, len);

                if (1/* FN */+ (p - buf) < len) {
                    codes_ir.emplace_back(CoderConst::FN);
                    for (const char * cbegin = buf; cbegin != p; ++cbegin) {
                        codes_ir.emplace_back(*cbegin);
                    }
                } else {
                    codes_ir.insert(codes_ir.end(),
                                    _tree._chunk[chunk_idx].data() + from,
                                    _tree._chunk[chunk_idx].data() + to);
                }
            }
        }

        Coder coder;
        return coder.encode(codes_ir);
    }
}