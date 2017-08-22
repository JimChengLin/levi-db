#include "compress.h"
#include "crc32c.h"
#include "log_reader.h"
#include "log_writer.h"
#include "varint.h"

namespace LeviDB {
    namespace LogReader {
        void defaultReporter(const Exception & e) {
            throw e;
        };

        class RawIterator : public SimpleIterator<Slice> {
        private:
            RandomAccessFile * _dst;
            reporter_t _reporter;
            Slice _item;
            uint32_t _cursor;
            bool _eof = false;
            char _backing_store[LogWriterConst::block_size_]{};

        public:
            RawIterator(RandomAccessFile * dst, uint32_t offset, reporter_t reporter)
                    : _dst(dst), _reporter(std::move(reporter)), _cursor(offset) { next(); }

            DEFAULT_MOVE(RawIterator);
            DELETE_COPY(RawIterator);

            ~RawIterator() noexcept override = default;

            EXPOSE(_cursor);

            bool valid() const override {
                return !_eof;
            };

            Slice item() const override {
                return _item;
            };

            void next() override {
                size_t n_block = _cursor / LogWriterConst::block_size_;
                size_t block_offset = _cursor % LogWriterConst::block_size_;
                size_t remaining_bytes = LogWriterConst::block_size_ - block_offset;

                // skip trailer
                if (block_offset != 0 && remaining_bytes < LogWriterConst::header_size_) {
                    _cursor += remaining_bytes;
                    return next();
                }

                try {
                    char buf[LogWriterConst::header_size_]{};
                    _dst->read(_cursor, LogWriterConst::header_size_, buf);
                    _cursor += LogWriterConst::header_size_;
                    remaining_bytes -= LogWriterConst::header_size_;

                    uint16_t length;
                    memcpy(&length, buf + 4/* checksum */+ 1/* type */, sizeof(length));
                    if (length > remaining_bytes) {
                        throw Exception::corruptionException("bad record length");
                    }

                    _dst->read(_cursor, length, _backing_store);
                    _cursor += length;
                    remaining_bytes -= length;

                    uint32_t calc_checksum = CRC32C::extend(CRC32C::value(buf + 4, 3), _backing_store, length);
                    uint32_t store_checksum;
                    memcpy(&store_checksum, buf, sizeof(store_checksum));
                    if (calc_checksum != store_checksum) {
                        throw Exception::corruptionException("checksum mismatch");
                    }

                    _backing_store[length] = buf[4]; // store meta char
                    _item = Slice(_backing_store, length + 1);
                } catch (const Exception & e) { // anything wrong? drop and report
                    _cursor += remaining_bytes;
                    if (e.isIOError()) {
                        _eof = true;
                    }
                    _reporter(e);
                }
            };
        };

        std::unique_ptr<SimpleIterator<Slice>>
        makeRawIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter) {
            return std::make_unique<RawIterator>(data_file, offset, std::move(reporter));
        }

        // RawIterator 的结尾是 meta char, 掩盖掉然后传给解压器
        class IteratorTrimLastChar : public SimpleIterator<Slice> {
        private:
            std::unique_ptr<SimpleIterator<Slice>> _raw_iter;

        public:
            explicit IteratorTrimLastChar(std::unique_ptr<SimpleIterator<Slice>> && raw_iter) noexcept
                    : _raw_iter(std::move(raw_iter)) {}

            DEFAULT_MOVE(IteratorTrimLastChar);
            DELETE_COPY(IteratorTrimLastChar);

            ~IteratorTrimLastChar() noexcept override = default;

            bool valid() const override {
                return _raw_iter->valid();
            };

            Slice item() const override {
                return {_raw_iter->item().data(), _raw_iter->item().size() - 1};
            };

            void next() override {
                _raw_iter->next();
            }
        };

        // 将压缩的流解压成普通的
        class UncompressIterator : public SimpleIterator<Slice> {
        private:
            RawIterator * _raw_iter_ob;
            std::unique_ptr<SimpleIterator<Slice>> _decode_iter;
            std::vector<uint8_t> _buffer;

        public:
            explicit UncompressIterator(std::unique_ptr<SimpleIterator<Slice>> && raw_iter)
                    : _raw_iter_ob(reinterpret_cast<RawIterator *>(raw_iter.get())),
                      _decode_iter(Compressor::makeDecodeIterator(
                              std::make_unique<IteratorTrimLastChar>(std::move(raw_iter)))) { next(); }

            DEFAULT_MOVE(UncompressIterator);
            DELETE_COPY(UncompressIterator);

            ~UncompressIterator() noexcept override = default;

            bool valid() const override {
                return _decode_iter->valid();
            };

            Slice item() const override {
                return {_buffer.data(), _buffer.size()};
            };

            void next() override {
                _buffer.clear();
                char type = _raw_iter_ob->item().back();
                uint32_t cursor = _raw_iter_ob->immut_cursor();
                while (_raw_iter_ob->immut_cursor() == cursor && _decode_iter->valid()) { // 正在解压原有 raw block
                    _buffer.insert(_buffer.end(),
                                   reinterpret_cast<const uint8_t *>(_decode_iter->item().data()),
                                   reinterpret_cast<const uint8_t *>(
                                           _decode_iter->item().data() + _decode_iter->item().size()));
                    _decode_iter->next();
                }
                _buffer.emplace_back(charToUint8(type));
            }
        };

        class RecordIterator : public Iterator<Slice, std::string> {
        protected:
            typedef std::pair<uint32_t, uint32_t> from_to;
            typedef std::pair<from_to, from_to> kv_pair;

            mutable std::unique_ptr<SimpleIterator<Slice>> _raw_iter;
            mutable std::vector<uint8_t> _buffer;
            reporter_t _reporter;

            std::vector<kv_pair> _rep;
            std::vector<kv_pair>::const_iterator _cursor;
            const bool _compress_format;

        public:
            RecordIterator(RandomAccessFile * dst, uint32_t offset, const reporter_t & reporter)
                    : _raw_iter(makeRawIterator(dst, offset, reporter)),
                      _reporter(reporter),
                      _cursor(_rep.cend()),
                      _compress_format(((_raw_iter->item().back() >> 4) & 1) == 1) {
                dependencyCheck(0b00001111, _raw_iter->item().back()); // 确保合法的起始位置
                if (_compress_format) {
                    _raw_iter = std::make_unique<UncompressIterator>(std::move(_raw_iter));
                }
            }

            DEFAULT_MOVE(RecordIterator);
            DELETE_COPY(RecordIterator);

            ~RecordIterator() noexcept override = default;

        public:
            bool valid() const override { return _cursor != _rep.cend(); };

            void seekToFirst() override {
                if (_rep.empty()) {
                    assert(_buffer.empty());

                    if (_compress_format) {

                    } else {

                    }
                }

                _cursor = _rep.cbegin();
            };

            void seekToLast() override {
                seekToFirst();
                _cursor = --_rep.cend();
            };

            void seek(const Slice & target) override {
                seekToFirst();
                while (valid()) {
                    if (!SliceComparator{}(key(), target)) {
                        break;
                    }
                    next();
                }
            };

            void next() override {
                ++_cursor;
            };

            void prev() override {
                if (_cursor == _rep.cbegin()) {
                    _cursor = _rep.cend();
                } else {
                    --_cursor;
                }
            };

            Slice key() const override {

            };

            std::string value() const override {

            };

        protected:
            void fetchData() const {
                _buffer.insert(_buffer.end(),
                               reinterpret_cast<const uint8_t *>(_raw_iter->item().data()),
                               reinterpret_cast<const uint8_t *>(
                                       _raw_iter->item().data() + _raw_iter->item().size() - 1));
                char type_a = _raw_iter->item().back();
                _raw_iter->next();
                if (_raw_iter->valid()) {
                    char type_b = _raw_iter->item().back();
                    dependencyCheck(type_a, type_b);
                }
            }

            void ensureDataLoaded(uint32_t length) const {
                while (length > _buffer.size()) {
                    fetchData();
                    if (!_raw_iter->valid()) {
                        break;
                    }
                }
                if (_buffer.size() < length) {
                    _reporter(Exception::IOErrorException("EOF"));
                }
            }

            // TableIterator has a different way to check
            virtual void dependencyCheck(char type_a, char type_b) const {

            }
        };

        std::unique_ptr<kv_iter>
        makeIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter) {
            return std::make_unique<RecordIterator>(data_file, offset, std::move(reporter));
        }

        class TableIterator : public RecordIterator {
        public:
            TableIterator(RandomAccessFile * dst, uint32_t offset, const reporter_t & reporter)
                    : RecordIterator(dst, offset, reporter) {}

        };

        std::unique_ptr<kv_iter>
        makeTableIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter) {
            return std::make_unique<TableIterator>(data_file, offset, std::move(reporter));
        }
    }
}