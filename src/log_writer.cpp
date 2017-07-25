#include "log_writer.h"
#include <bitset>

namespace LeviDB {
    void LogWriter::addRecords(const std::vector<Record> & records, bool compress) {
        bool record_begin = true;
        for (const Record & record:records) {
            Slice key;
            Slice val;
            bool del;
            std::tie(key, val, del) = record;

            if (del) {
                _compressor.submitDel(key);
            } else {
                _compressor.submit(key, val);
            }

            bool kv_begin = true;
            do {
                const int leftover = LogWriterConst::block_size - _block_offset;
                assert(leftover >= 0);
                if (leftover < LogWriterConst::min_size) {
                    if (leftover > 0) {
                        static_assert(LogWriterConst::min_size == 7, "trailing bytes may be wrong");
                        _dst->append(Slice("\x00\x00\x00\x00\x00\x00", static_cast<size_t>(leftover)));
                    }
                    _block_offset = 0;
                }

                const int avail = LogWriterConst::block_size - _block_offset - LogWriterConst::header_size;
                const auto bin = _compressor.next(static_cast<uint16_t>(avail), compress,
                                                  static_cast<uint32_t>(_dst->length()));

                LogWriterConst::ConcatType kv_type;
                const bool kv_end = !_compressor.valid();
                if (kv_begin && kv_end) {
                    kv_type = LogWriterConst::FULL;
                } else if (kv_begin) {
                    kv_type = LogWriterConst::FIRST;
                } else if (kv_end) {
                    kv_type = LogWriterConst::LAST;
                } else {
                    kv_type = LogWriterConst::MIDDLE;
                }

                LogWriterConst::ConcatType record_type;
                const bool record_end = (&record == &records.back());
                if (record_begin && record_end) {
                    record_type = LogWriterConst::FULL;
                } else if (record_begin) {
                    record_type = LogWriterConst::FIRST;
                } else if (record_end) {
                    record_type = LogWriterConst::LAST;
                } else {
                    record_type = LogWriterConst::MIDDLE;
                }

                emitPhysicalRecord(getCombinedType(record_type, kv_type, bin.second),
                                   Slice(bin.first.data(), bin.first.size()));
                kv_begin = false;
            } while (_compressor.valid());
            record_begin = false;
        }
    }

    void LogWriter::emitPhysicalRecord(uint8_t type, const Slice & data) {
        char buf[3];
        buf[0] = type;
        auto tmp = static_cast<uint16_t>(data.size());
        memcpy(&buf[1], &tmp, sizeof(tmp));
        _dst->append(Slice(buf, 3));
        _dst->append(data);
        _dst->flush();
    }

    uint8_t LogWriter::getCombinedType(LogWriterConst::ConcatType record_type,
                                       LogWriterConst::ConcatType kv_type,
                                       CompressorConst::CompressType compress_type) noexcept {
        std::bitset<8> res;

        int base = 0;
        for (LogWriterConst::ConcatType type:{record_type, kv_type}) {
            switch (type) {
                case LogWriterConst::FULL:
                    break;
                case LogWriterConst::FIRST: // 0b01
                    res[base + 1] = true;
                    break;
                case LogWriterConst::MIDDLE: // 0b10
                    res[base] = true;
                    break;
                case LogWriterConst::LAST: // 0b11
                    res[base] = true;
                    res[base + 1] = true;
                    break;
            }
            base += 2;
        }

        switch (compress_type) {
            case CompressorConst::NO_COMPRESS:
                break;
            case CompressorConst::CODER_COMPRESS:
                res[4] = true;
                break;
            case CompressorConst::SIMPLE_COMPRESS:
                res[4] = true;
                res[5] = true;
                break;
        }

        res[6] = static_cast<bool>(res[1] ^ res[3] ^ res[5]);
        res[7] = static_cast<bool>(res[0] ^ res[2] ^ res[4]);
        return static_cast<uint8_t>(res.to_ulong());
    }
}