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
            Slice _item;
            reporter_t _reporter;
            uint32_t _cursor;
            bool _eof = false;
            char _backing_store[LogWriterConst::block_size_]{};

        public:
            RawIterator(RandomAccessFile * dst, uint32_t offset, reporter_t reporter)
                    : _dst(dst), _reporter(std::move(reporter)), _cursor(offset) { next(); }

            DEFAULT_MOVE(RawIterator);
            DELETE_COPY(RawIterator);

            ~RawIterator() noexcept override = default;

            EXPOSE(_cursor); // for ob

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

                    uint32_t calc_checksum = CRC32C::extend(CRC32C::value(buf + 4, 1 + 2), _backing_store, length);
                    uint32_t store_checksum;
                    memcpy(&store_checksum, buf, sizeof(store_checksum));
                    if (calc_checksum != store_checksum) {
                        throw Exception::corruptionException("checksum mismatch");
                    }

                    // store meta char
                    _backing_store[length] = buf[4];
                    _item = Slice(_backing_store, length + 1);
                } catch (const Exception & e) { // anything wrong? drop and report
                    if (e.isIOError()) {
                        _eof = true;
                    }
                    _reporter(e);
                    _cursor += remaining_bytes;
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

        // 将压缩的流解压
        class UncompressIterator : public SimpleIterator<Slice> {
        private:
            RawIterator * _raw_iter_ob;
            std::unique_ptr<SimpleIterator<Slice>> _decode_iter;
            std::vector<uint8_t> _buffer;

        public:
            explicit UncompressIterator(std::unique_ptr<SimpleIterator<Slice>> && raw_iter)
                    : _raw_iter_ob(static_cast<RawIterator *>(raw_iter.get())),
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
                while (_raw_iter_ob->immut_cursor() == cursor && _decode_iter->valid()) { // 必须解压完当前 block
                    _buffer.insert(_buffer.end(),
                                   reinterpret_cast<const uint8_t *>(_decode_iter->item().data()),
                                   reinterpret_cast<const uint8_t *>(
                                           _decode_iter->item().data() + _decode_iter->item().size()));
                    _decode_iter->next();
                }
                _buffer.emplace_back(charToUint8(type));
            }
        };

        static inline bool is_record_full(char t) noexcept {
            return ((t >> 2) & 1) == 0 && ((t >> 3) & 1) == 0;
        }

        static inline bool is_record_first(char t) noexcept {
            return ((t >> 2) & 1) == 1 && ((t >> 3) & 1) == 0;
        }

        static inline bool is_record_middle(char t) noexcept {
            return ((t >> 2) & 1) == 0 && ((t >> 3) & 1) == 1;
        }

        static inline bool is_record_last(char t) noexcept {
            return ((t >> 2) & 1) == 1 && ((t >> 3) & 1) == 1;
        }

        static inline bool is_record_compress(char t) noexcept {
            return ((t >> 4) & 1) == 1;
        }

        static inline bool is_record_del(char t) noexcept {
            return ((t >> 5) & 1) == 1;
        }

        class RecordIteratorBase {
        private:
            mutable std::unique_ptr<SimpleIterator<Slice>> _raw_iter;
            mutable std::vector<uint8_t> _buffer;
            mutable bool _meet_all = false;

        protected:
            explicit RecordIteratorBase(std::unique_ptr<SimpleIterator<Slice>> && raw_iter)
                    : _raw_iter(std::move(raw_iter)) { initCheck(_raw_iter->item().back()); }

            EXPOSE(_buffer);

            EXPOSE(_meet_all);

            void ensureDataLoad(uint32_t length) const {
                while (!_meet_all && length > _buffer.size() && _raw_iter->valid()) {
                    fetchData();
                }
                if (_buffer.size() < length) {
                    throw Exception::IOErrorException("EOF / meet all too early");
                }
            }

        private:
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

            void dependencyCheck(char type_a, char type_b) const {
                if (is_record_full(type_a) || is_record_last(type_a)) {
                    _meet_all = true;
                    return;
                }
                if (is_record_middle(type_b) || is_record_last(type_b)) {
                    return;
                }
                throw Exception::corruptionException("fragmented record");
            }

            void initCheck(char type) const {
                if (is_record_full(type) || is_record_first(type)) {
                } else {
                    throw Exception::corruptionException("missing start of fragmented record");
                };
            }
        };

        class RecordIterator : public RecordIteratorBase, public Iterator<Slice, std::string> {
        private:
            uint32_t _k_len = 0;
            uint8_t _k_from = 0;
            bool _v_load = false;
            bool _done = true;
            bool _del;

        public:
            explicit RecordIterator(std::unique_ptr<SimpleIterator<Slice>> && raw_iter)
                    : RecordIteratorBase(std::move(raw_iter)), _del(is_record_del(raw_iter->item().back())) {}

            DEFAULT_MOVE(RecordIterator);
            DELETE_COPY(RecordIterator);

            ~RecordIterator() noexcept override = default;

        public:
            bool valid() const override { return !_done; };

            void seekToFirst() override {
                if (_k_len == 0) {
                    assert(_k_from == 0);
                    while (true) {
                        ensureDataLoad(++_k_from);
                        if (decodeVarint32(reinterpret_cast<const char *>(immut_buffer().data()),
                                           reinterpret_cast<const char *>(immut_buffer().data() + _k_from),
                                           &_k_len) != nullptr) {
                            break;
                        }
                    }
                }
                _done = false;
            };

            void seekToLast() override {
                seekToFirst();
            };

            void seek(const Slice & target) override {
                seekToFirst();
                if (SliceComparator{}(key(), target)) {
                    _done = true;
                }
            };

            void next() override {
                _done = true;
            };

            void prev() override {
                next();
            };

            Slice key() const override {
                ensureDataLoad(_k_from + _k_len);
                return {&immut_buffer()[_k_from], _k_len};
            };

            std::string value() const override {
                if (!_v_load) {
                    assert(immut_buffer().empty());
                    while (!immut_meet_all()) {
                        ensureDataLoad(static_cast<uint32_t>(immut_buffer().size() + 1));
                    }
                }
                return std::string(reinterpret_cast<const char *>(&immut_buffer()[_k_from + _k_len]),
                                   reinterpret_cast<const char *>(&immut_buffer().back() + 1))
                       + static_cast<char>(_del);
            };
        };

        class RecordIteratorCompress : public RecordIteratorBase, public Iterator<Slice, std::string> {
        protected:
            typedef std::pair<uint32_t, uint32_t> from_to;
            typedef std::pair<from_to, from_to> kv_pair;

            std::vector<kv_pair> _rep;
            std::vector<kv_pair>::const_iterator _cursor;

        public:
            explicit RecordIteratorCompress(std::unique_ptr<SimpleIterator<Slice>> && raw_iter)
                    : RecordIteratorBase(std::move(raw_iter)), _cursor(_rep.cend()) {}

            DEFAULT_MOVE(RecordIteratorCompress);
            DELETE_COPY(RecordIteratorCompress);

            ~RecordIteratorCompress() noexcept override = default;

        public:
            bool valid() const override { return _cursor != _rep.cend(); };

            void seekToFirst() override {
                if (_rep.empty()) {
                    assert(immut_buffer().empty());

                    uint16_t meta_len;
                    ensureDataLoad(sizeof(meta_len));
                    memcpy(&meta_len, &immut_buffer()[0], sizeof(meta_len));

                    ensureDataLoad(sizeof(meta_len) + meta_len);
                    std::vector<from_to> ranges;
                    const auto * p = reinterpret_cast<const char *>(&immut_buffer()[sizeof(meta_len)]);
                    const auto * limit = reinterpret_cast<const char *>(&immut_buffer()[sizeof(meta_len) + meta_len]);

                    uint32_t offset = sizeof(meta_len) + meta_len;
                    while (p != limit) {
                        uint32_t val;
                        p = decodeVarint32(p, limit, &val);
                        if (p == nullptr) {
                            throw Exception::corruptionException("");
                        }
                        ranges.emplace_back(from_to(offset, offset + val));
                        offset += val;
                    }
                    for (int i = 0; i < ranges.size() / 2; ++i) {
                        _rep.emplace_back(kv_pair(ranges[i], ranges[i + ranges.size() / 2]));
                    }
                }

                _cursor = _rep.cbegin();
            };

            void seekToLast() override {
                seekToFirst();
                _cursor = --_rep.cend();
            };

            void seek(const Slice & target) override {
                if (target == key()) {
                    return;
                }
                seekToFirst();
                while (valid() && SliceComparator{}(key(), target)) {
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
                const auto kv = *_cursor;
                const auto k_from_to = kv.first;
                ensureDataLoad(k_from_to.second);
                return {&immut_buffer()[k_from_to.first], k_from_to.second - k_from_to.first};
            };

            std::string value() const override {
                const auto kv = *_cursor;
                const auto v_from_to = kv.second;
                ensureDataLoad(v_from_to.second);
                return std::string(reinterpret_cast<const char *>(&immut_buffer()[v_from_to.first]),
                                   v_from_to.second - v_from_to.first)
                       + static_cast<char>(false);
            };
        };

        std::unique_ptr<kv_iter>
        makeIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter) {
            auto raw_iter = makeRawIterator(data_file, offset, std::move(reporter));
            if (is_record_compress(raw_iter->item().back())) {
                return std::make_unique<RecordIteratorCompress>(std::move(raw_iter));
            }
            return std::make_unique<RecordIterator>(std::move(raw_iter));
        }
    }
}