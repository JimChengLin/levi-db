#include "compress.h"
#include "crc32c.h"
#include "env_io.h"
#include "log_writer.h"
#include "varint.h"

namespace levidb8 {
    LogWriter::LogWriter(AppendableFile * dst) noexcept : _dst(dst) {
        assert(_dst->immut_length() == 0);
    }

    LogWriter::LogWriter(AppendableFile * dst, uint64_t dst_len) noexcept
            : _dst(dst), _block_offset(dst_len % kLogBlockSize) {}

    uint32_t LogWriter::addRecord(const Slice & bkv) {
        return addRecordTpl<ADD_RECORD>(bkv);
    }

    uint32_t LogWriter::addRecordForDel(const Slice & bkv) {
        return addRecordTpl<ADD_RECORD_FOR_DEL>(bkv);
    }

    uint32_t LogWriter::addCompressedRecords(const Slice & bkvs) {
        return addRecordTpl<ADD_COMPRESSED_RECORDS>(bkvs);
    }

    std::vector<uint32_t>
    LogWriter::addRecordsMayDel(const Slice * bkvs, size_t n, std::vector<uint32_t> addrs) {
        std::lock_guard<std::mutex> guard(_emit_lock);
        addrs.resize(n);

        bool record_begin = true;
        for (size_t i = 0; i < n; ++i) {
            const Slice & bkv = bkvs[i];

            ConcatType record_type = FULL;
            const bool record_end = (i == n - 1);
            if (record_begin && record_end) {
            } else if (record_begin) {
                record_type = FIRST;
            } else if (record_end) {
                record_type = LAST;
            } else {
                record_type = MIDDLE;
            }
            record_begin = false;

            addrs[i] = static_cast<bool>(addrs[i]) ? addRecordTpl<ADD_RECORD_FOR_DEL, false>(bkv, record_type)
                                                   : addRecordTpl<ADD_RECORD, false>(bkv, record_type);
        }
        _dst->flush();
        return addrs;
    }

    struct PlaceHolder {
        template<typename ...PARAMS>
        explicit PlaceHolder(PARAMS && ...) noexcept {};
    };

    template<LogWriter::Type TYPE, bool LOCK>
    uint32_t LogWriter::addRecordTpl(const Slice & bkv, ConcatType record_type) {
        using guard_t = typename std::conditional<LOCK, std::lock_guard<std::mutex>, PlaceHolder>::type;
        guard_t guard(_emit_lock);

        restart:
        static_assert(kLogBlockSize % kPageSize == 0, "never clash");
        if (_block_offset % kPageSize == 0) {
            _dst->append({"\x00", 1});
            ++_block_offset;
            if (_block_offset == kLogBlockSize + 1) {
                _block_offset = 1;
            }
        }
        if (_dst->immut_length() > kLogFileLimit) {
            throw LogFullControlledException();
        }

        const auto pos = static_cast<uint32_t>(_dst->immut_length());
        const char * ptr = bkv.data();
        size_t left = bkv.size();

        bool kv_begin = true;
        do {
            const size_t leftover = kLogBlockSize - _block_offset;
            if (leftover < kLogHeaderSize) {
                if (leftover > 0) {
                    static_assert(kLogHeaderSize == 7, "trailing bytes are not enough");
                    _dst->append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
                    if (kv_begin) {
                        _block_offset = 0;
                        goto restart;
                    }
                }
                _block_offset = 0;
            }

            const size_t avail = kLogBlockSize - _block_offset - kLogHeaderSize;
            const size_t fragment_length = std::min(left, avail);

            ConcatType kv_type = FULL;
            const bool kv_end = (fragment_length == left);
            if (kv_begin && kv_end) {
            } else if (kv_begin) {
                kv_type = FIRST;
            } else if (kv_end) {
                kv_type = LAST;
            } else {
                kv_type = MIDDLE;
            }
            kv_begin = false;

            emitPhysicalRecord(getCombinedType(record_type, kv_type,
                                               TYPE == ADD_COMPRESSED_RECORDS,
                                               TYPE == ADD_RECORD_FOR_DEL), ptr, fragment_length);
            ptr += fragment_length;
            left -= fragment_length;
        } while (left > 0);

        if (LOCK) {
            _dst->flush();
        }
        return pos;
    }

    std::bitset<8> LogWriter::getCombinedType(ConcatType record_type, ConcatType kv_type,
                                              bool compress, bool del) const noexcept {
        std::bitset<8> res;

        int base = 0;
        for (ConcatType type:{record_type, kv_type}) {
            switch (type) {
                case FULL: // 0b00
                    break;
                case FIRST: // 0b01
                    res[base] = true;
                    break;
                case MIDDLE: // 0b10
                    res[base + 1] = true;
                    break;
                case LAST: // 0b11
                    res[base] = true;
                    res[base + 1] = true;
                    break;
            }
            base += 2;
        }

        res[4] = compress;
        res[5] = del;
        return res;
    }

    void LogWriter::emitPhysicalRecord(std::bitset<8> type, const char * ptr, size_t length) {
        assert(kLogHeaderSize + _block_offset + length <= kLogBlockSize);

        char buf[kLogHeaderSize];
        buf[4] = uint8ToChar(static_cast<uint8_t>(type.to_ulong()));
        buf[5] = static_cast<char>(length & 0xff);
        buf[6] = static_cast<char>(length >> 8);

        uint32_t crc = crc32c::extend(crc32c::value(&buf[4], 3), ptr, length);
        memcpy(buf, &crc, sizeof(crc));

        _dst->append(Slice(buf, kLogHeaderSize));
        _dst->append(Slice(ptr, length));
        _block_offset += kLogHeaderSize + length;
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

    std::vector<uint8_t> LogWriter::makeCompressedRecords(const std::pair<Slice, Slice> * kvs, size_t n) {
        size_t bin_size = 0;
        std::vector<uint8_t> src(sizeof(uint16_t));
        for (size_t i = 0; i < n; ++i) {
            const auto kv = kvs[i];
            bin_size += kv.first.size();
            char buf[5];
            char * p = encodeVarint32(buf, static_cast<uint32_t>(kv.first.size()));
            src.insert(src.end(), reinterpret_cast<uint8_t *>(buf), reinterpret_cast<uint8_t *>(p));
        }
        for (size_t i = 0; i < n; ++i) {
            const auto kv = kvs[i];
            assert(kv.second.data() != nullptr); // nullptr = batch-style del
            bin_size += kv.second.size();
            char buf[5];
            char * p = encodeVarint32(buf, static_cast<uint32_t>(kv.second.size()));
            src.insert(src.end(), reinterpret_cast<uint8_t *>(buf), reinterpret_cast<uint8_t *>(p));
        }

        auto meta_size = static_cast<uint16_t>(src.size() - sizeof(uint16_t));
        memcpy(&src[0], &meta_size, sizeof(meta_size));

        src.reserve(src.size() + bin_size);
        for (size_t i = 0; i < n; ++i) {
            const auto kv = kvs[i];
            src.insert(src.end(),
                       reinterpret_cast<const uint8_t *>(kv.first.data()),
                       reinterpret_cast<const uint8_t *>(kv.first.data() + kv.first.size()));
        }
        for (size_t i = 0; i < n; ++i) {
            const auto kv = kvs[i];
            src.insert(src.end(),
                       reinterpret_cast<const uint8_t *>(kv.second.data()),
                       reinterpret_cast<const uint8_t *>(kv.second.data() + kv.second.size()));
        }
        return compress::encode(src);
    }
}
