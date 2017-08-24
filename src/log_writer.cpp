#include "compress.h"
#include "crc32c.h"
#include "log_writer.h"
#include "varint.h"

namespace LeviDB {
    uint32_t LogWriter::calcWritePos() const noexcept {
        const size_t leftover = LogWriterConst::block_size_ - _block_offset;
        return static_cast<uint32_t>(_dst->immut_length()
                                     + (leftover < LogWriterConst::header_size_ ? leftover : 0));
    }

    void LogWriter::addRecords(const std::vector<Slice> & bkvs, bool compress, bool del,
                               std::vector<uint32_t> * addrs) {
        bool record_begin = true;
        for (const Slice & bkv:bkvs) {
            if (addrs != nullptr) addrs->emplace_back(_dst->immut_length());

            const char * ptr = bkv.data();
            size_t left = bkv.size();

            bool kv_begin = true;
            do {
                const size_t leftover = LogWriterConst::block_size_ - _block_offset;
                if (leftover < LogWriterConst::header_size_) {
                    if (leftover > 0) {
                        static_assert(LogWriterConst::header_size_ == 7, "trailing bytes are not enough");
                        _dst->append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
                    }
                    _block_offset = 0;
                }

                const size_t avail = LogWriterConst::block_size_ - _block_offset - LogWriterConst::header_size_;
                const size_t fragment_length = std::min(left, avail);

                LogWriterConst::ConcatType record_type = LogWriterConst::FULL;
                const bool record_end = (&bkv == &bkvs.back());
                if (record_begin && record_end) {
                } else if (record_begin) {
                    record_type = LogWriterConst::FIRST;
                } else if (record_end) {
                    record_type = LogWriterConst::LAST;
                } else {
                    record_type = LogWriterConst::MIDDLE;
                }

                LogWriterConst::ConcatType kv_type = LogWriterConst::FULL;
                const bool kv_end = (fragment_length == left);
                if (kv_begin && kv_end) {
                } else if (kv_begin) {
                    kv_type = LogWriterConst::FIRST;
                } else if (kv_end) {
                    kv_type = LogWriterConst::LAST;
                } else {
                    kv_type = LogWriterConst::MIDDLE;
                }

                emitPhysicalRecord(getCombinedType(record_type, kv_type, compress, del), ptr, fragment_length);
                ptr += fragment_length;
                left -= fragment_length;

                kv_begin = false;
            } while (left > 0);
            record_begin = false;
        }
    }

    std::bitset<8> LogWriter::getCombinedType(LogWriterConst::ConcatType record_type,
                                              LogWriterConst::ConcatType kv_type,
                                              bool compress, bool del) const noexcept {
        std::bitset<8> res;

        int base = 0;
        for (LogWriterConst::ConcatType type:{record_type, kv_type}) {
            switch (type) {
                case LogWriterConst::FULL: // 0b00
                    break;
                case LogWriterConst::FIRST: // 0b01
                    res[base] = true;
                    break;
                case LogWriterConst::MIDDLE: // 0b10
                    res[base + 1] = true;
                    break;
                case LogWriterConst::LAST: // 0b11
                    res[base] = true;
                    res[base + 1] = true;
                    break;
            }
            base += 2;
        }

        res[4] = compress;
        res[5] = del;

        res[6] = static_cast<bool>(res[1] ^ res[3] ^ static_cast<int>(res[5]));
        res[7] = static_cast<bool>(res[0] ^ res[2] ^ static_cast<int>(res[4]));
        return res;
    }

    void LogWriter::emitPhysicalRecord(std::bitset<8> type, const char * ptr, size_t length) {
        assert(length <= 32768); // 2^15
        assert(LogWriterConst::header_size_ + _block_offset + length <= LogWriterConst::block_size_);

        char buf[LogWriterConst::header_size_];
        buf[4] = uint8ToChar(static_cast<uint8_t>(type.to_ulong()));
        buf[5] = static_cast<char>(length & 0xff);
        buf[6] = static_cast<char>(length >> 8);

        uint32_t crc = CRC32C::extend(CRC32C::value(&buf[4], 3), ptr, length);
        memcpy(buf, &crc, sizeof(crc));

        _dst->append(Slice(buf, LogWriterConst::header_size_));
        _dst->append(Slice(ptr, length));
        _dst->flush();
        _block_offset += LogWriterConst::header_size_ + length;
    }

    std::vector<uint8_t> LogWriter::makeRecord(const Slice & k, const Slice & v) noexcept {
        char buf[5];
        char * p = encodeVarint32(buf, static_cast<uint32_t>(k.size()));

        std::vector<uint8_t> res;
        res.reserve((p - buf) + k.size() + v.size());

        res.insert(res.end(), reinterpret_cast<uint8_t *>(buf), reinterpret_cast<uint8_t *>(p));
        res.insert(res.end(),
                   reinterpret_cast<const uint8_t *>(k.data()),
                   reinterpret_cast<const uint8_t *>(k.data() + k.size()));
        res.insert(res.end(),
                   reinterpret_cast<const uint8_t *>(v.data()),
                   reinterpret_cast<const uint8_t *>(v.data() + v.size()));
        return res;
    }

    std::vector<uint8_t> LogWriter::makeCompressRecord(const std::vector<std::pair<Slice, Slice>> & kvs) noexcept {
        size_t bin_size = 0;
        std::vector<uint8_t> src(sizeof(uint16_t));
        for (const auto & kv:kvs) {
            bin_size += kv.first.size();
            char buf[5];
            char * p = encodeVarint32(buf, static_cast<uint32_t>(kv.first.size()));
            src.insert(src.end(), reinterpret_cast<uint8_t *>(buf), reinterpret_cast<uint8_t *>(p));
        }
        for (const auto & kv:kvs) {
            bin_size += kv.second.size();
            char buf[5];
            char * p = encodeVarint32(buf, static_cast<uint32_t>(kv.second.size()));
            src.insert(src.end(), reinterpret_cast<uint8_t *>(buf), reinterpret_cast<uint8_t *>(p));
        }

        auto meta_size = static_cast<uint16_t>(src.size() - sizeof(uint16_t));
        memcpy(&src[0], &meta_size, sizeof(meta_size));

        src.reserve(src.size() + bin_size);
        for (const auto & kv:kvs) {
            src.insert(src.end(),
                       reinterpret_cast<const uint8_t *>(kv.first.data()),
                       reinterpret_cast<const uint8_t *>(kv.first.data() + kv.first.size()));
        }
        for (const auto & kv:kvs) {
            src.insert(src.end(),
                       reinterpret_cast<const uint8_t *>(kv.second.data()),
                       reinterpret_cast<const uint8_t *>(kv.second.data() + kv.second.size()));
        }
        return Compressor::encode({src.data(), src.size()});
    }
}