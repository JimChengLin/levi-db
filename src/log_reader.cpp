#include "compress.h"
#include "config.h"
#include "crc32c.h"
#include "env_io.h"
#include "log_reader.h"
#include "optional.h"
#include "varint.h"

namespace levidb8 {
    namespace log_reader {
        static inline bool isRecordFull(char t) noexcept {
            return (t & (0b11 << 2)) == (0b00 << 2);
        }

        static inline bool isRecordFirst(char t) noexcept {
            return (t & (0b11 << 2)) == (0b01 << 2);
        }

        static inline bool isRecordMiddle(char t) noexcept {
            return (t & (0b11 << 2)) == (0b10 << 2);
        }

        static inline bool isRecordLast(char t) noexcept {
            return (t & (0b11 << 2)) == (0b11 << 2);
        }

        static inline bool isRecordCompress(char t) noexcept {
            return (t & (1 << 4)) == (1 << 4);
        }

        static inline bool isRecordDel(char t) noexcept {
            return (t & (1 << 5)) == (1 << 5);
        }

        static inline bool isBatchFull(char t) noexcept {
            return (t & 0b11) == 0b00;
        }

        static inline bool isBatchFirst(char t) noexcept {
            return (t & 0b11) == 0b01;
        }

        static inline bool isBatchMiddle(char t) noexcept {
            return (t & 0b11) == 0b10;
        }

        static inline bool isBatchLast(char t) noexcept {
            return (t & 0b11) == 0b11;
        }

        using RawIterator = SimpleIterator<std::pair<Slice/* page */, std::pair<uint32_t/* offset */, char/* type */>>>;

        class Connector;

        class KeyValueIterator : public Iterator<Slice, std::pair<Slice, Meta>> {
        public:
            virtual Connector yieldConnector() = 0;
        };

        using KeyOffsetSimpleIterator = SimpleIterator<std::pair<Slice, std::pair<uint32_t, Meta>>>;
        using KeyValueSimpleIterator = SimpleIterator<std::pair<Slice, std::pair<Slice, Meta>>>;

        class RawIteratorNoTypeCheck : public RawIterator {
        private:
            RandomAccessFile * _dst;
            uint32_t _cursor;
            Slice _page;
            char _backing_store[kLogBlockSize]{};
            char _type = 0;
            bool _eof = false;

        public:
            RawIteratorNoTypeCheck(RandomAccessFile * dst, uint32_t offset) noexcept
                    : _dst(dst), _cursor(offset) {}

            ~RawIteratorNoTypeCheck() noexcept override = default;

            bool valid() const override {
                return !_eof;
            }

            void prepare() override {}

            void next() override {
                restart:
                bool pad = (_cursor % kPageSize == 0 && (isRecordLast(_type) || isRecordFull(_type))
                            && _page.size() != 0);
                _cursor += static_cast<uint32_t>(pad);
                size_t block_offset = _cursor % kLogBlockSize;
                size_t remaining_bytes = kLogBlockSize - block_offset;

                if (remaining_bytes < kLogHeaderSize) {
                    _cursor += remaining_bytes;
                    goto restart;
                }

                char buf[kLogHeaderSize]{};
                if (_dst->read(_cursor, kLogHeaderSize, buf).size() != kLogHeaderSize) {
                    _eof = true;
                    return;
                }
                _cursor += kLogHeaderSize;
                remaining_bytes -= kLogHeaderSize;

                uint16_t length;
                memcpy(&length, buf/* start */ + 4/* checksum */+ 1/* type */, sizeof(length));
                if (length > remaining_bytes) {
                    throw Exception::corruptionException("bad record length");
                }

                if (_dst->read(_cursor, length, _backing_store).size() != length) {
                    _eof = true;
                    return;
                };
                _cursor += length;

                uint32_t calc_checksum = crc32c::extend(crc32c::value(buf + 4, 1 + sizeof(length)),
                                                        _backing_store, length);
                uint32_t store_checksum;
                memcpy(&store_checksum, buf, sizeof(store_checksum));
                if (calc_checksum != store_checksum) {
                    throw Exception::corruptionException("checksum mismatch");
                }

                _type = buf[4];
                _page = Slice(_backing_store, length);
            }

            std::pair<Slice, std::pair<uint32_t, char>>
            item() const override {
                assert(valid());
                return {_page, std::make_pair(_cursor - _page.size() - kLogHeaderSize, _type)};
            };
        };

        class RawIteratorCheckOnFly : public RawIterator {
        private:
            RawIteratorNoTypeCheck _iter;
            char _prev_type = 0; // FULL

        public:
            RawIteratorCheckOnFly(RandomAccessFile * dst, uint32_t offset) noexcept
                    : _iter(dst, offset) {}

            ~RawIteratorCheckOnFly() noexcept override = default;

            bool valid() const override {
                return _iter.valid();
            }

            void prepare() override {
                _iter.prepare();
            }

            void next() override {
                _iter.next();
                checkedOrThrow();
            }

            std::pair<Slice, std::pair<uint32_t, char>> item() const override {
                return _iter.item();
            }

        private:
            void checkedOrThrow() {
                if (valid()) {
                    char type = item().second.second;
                    dependencyCheck(_prev_type, type);
                    _prev_type = type;
                }
            }

            static void dependencyCheck(char type_a, char type_b) {
                if (isRecordFull(type_a) || isRecordLast(type_a)) { // prev is completed
                    if (isRecordFull(type_b) || isRecordFirst(type_b)) {
                        return;
                    }
                }
                if (isRecordFirst(type_a) || isRecordMiddle(type_a)) { // prev is starting
                    if (isRecordMiddle(type_b) || isRecordLast(type_b)) {
                        // same compress, same del
                        if (((type_a ^ type_b) & (0b11 << 4)) == 0) {
                            return;
                        }
                    }
                }
                throw Exception::corruptionException("fragmented record");
            }
        };

        class RawIteratorCheckBatch : public RawIterator {
        private:
            RawIteratorCheckOnFly _iter;
            std::vector<std::pair<std::vector<uint8_t>, std::pair<uint32_t, char>>> _buffer;
            std::vector<std::pair<std::vector<uint8_t>, std::pair<uint32_t, char>>>::const_iterator _buffer_cursor;
            char _prev_type = 0;

        public:
            RawIteratorCheckBatch(RandomAccessFile * dst, uint32_t offset) noexcept
                    : _iter(dst, offset), _buffer_cursor(_buffer.cend()) {}

            ~RawIteratorCheckBatch() noexcept override = default;

            bool valid() const override {
                return _buffer_cursor != _buffer.cend();
            }

            void prepare() override {
                _iter.prepare();
            }

            void next() override {
                if (_buffer_cursor == _buffer.cend() || ++_buffer_cursor == _buffer.cend()) {
                    _buffer.clear();

                    while (true) {
                        _iter.next();
                        if (!_iter.valid()) {
                            _buffer_cursor = _buffer.cend();
                            break; // invalid
                        }

                        auto item = _iter.item();
                        const Slice & page = item.first;
                        char type = item.second.second;

                        dependencyCheck(_prev_type, type);
                        _prev_type = type;

                        _buffer.emplace_back(std::vector<uint8_t>(
                                reinterpret_cast<const uint8_t *>(page.data()),
                                reinterpret_cast<const uint8_t *>(page.data() + page.size())),
                                             item.second);

                        if (isBatchFull(type) || isBatchLast(type)) {
                            _buffer_cursor = _buffer.cbegin();
                            break;
                        }
                    }
                }
            }

            std::pair<Slice, std::pair<uint32_t, char>> item() const override {
                assert(valid());
                return {{_buffer_cursor->first.data(), _buffer_cursor->first.size()}, _buffer_cursor->second};
            }

        private:
            static void dependencyCheck(char type_a, char type_b) {
                if (isBatchFull(type_a) || isBatchLast(type_a)) {
                    if (isBatchFull(type_b) || isBatchFirst(type_b)) {
                        return;
                    }
                }
                if (isBatchFirst(type_a) || isBatchMiddle(type_a)) {
                    if (isBatchMiddle(type_b) || isBatchLast(type_b)) {
                        return;
                    }
                }
                throw Exception::corruptionException("fragmented batch");
            }
        };

        class DecompressHelper : public SimpleIterator<Slice> {
        private:
            Slice * _src;

        public:
            explicit DecompressHelper(Slice * src) noexcept : _src(src) {}

            ~DecompressHelper() noexcept override = default;

            bool valid() const override { return _src != nullptr; }

            void prepare() override {}

            void next() override {}

            Slice item() const override {
                assert(valid());
                return *_src;
            }
        };

        class Decompressor {
        private:
            Slice _src;
            std::unique_ptr<SimpleIterator<Slice>> _decode_iter;

        public:
            Decompressor() : _decode_iter(
                    compressor::makeDecodeIterator(std::make_unique<DecompressHelper>(&_src))) {
                _decode_iter->prepare();
            }

            Slice submit(const Slice & code) {
                _src = code;
                _decode_iter->next();
                assert(_decode_iter->valid());
                return _decode_iter->item();
            }

            void reset() {
                _decode_iter = compressor::makeDecodeIterator(std::make_unique<DecompressHelper>(&_src));
                _decode_iter->prepare();
            }
        };

        class Connector {
        private:
            std::unique_ptr<RawIterator> _raw_iter;
            std::vector<uint8_t> _buffer;
            Optional<Decompressor> _decompressor;

            uint32_t _offset{kDiskNull};
            char _type{}; // 只需要 compress 和 del
            bool _met_all = false;

            friend class NormalRecordIterator;

            friend class CompressedRecordsIterator;

            friend class TableIterator;

        public:
            explicit Connector(std::unique_ptr<RawIterator> && raw_iter)
                    : _raw_iter(std::move(raw_iter)) {
                _raw_iter->prepare();
            }

            EXPOSE(_type);

            void ensureLoad(uint32_t length) {
                load(length);
                if (_buffer.size() < length) {
                    throw Exception::IOErrorException("EOF early");
                }
            }

            void load(uint32_t length) {
                while (length > _buffer.size() && !_met_all) {
                    _raw_iter->next();
                    if (!_raw_iter->valid()) {
                        break;
                    }

                    auto item = _raw_iter->item();
                    const Slice & page = item.first;
                    if (_offset == kDiskNull) {
                        _offset = item.second.first;
                    }
                    _type = item.second.second;

                    Slice content = isRecordCompress(_type)
                                    ? (_decompressor.valid()
                                       ? void()
                                       : _decompressor.build(), _decompressor->submit(page))
                                    : page;
                    _buffer.insert(_buffer.end(),
                                   reinterpret_cast<const uint8_t *>(content.data()),
                                   reinterpret_cast<const uint8_t *>(content.data() + content.size()));

                    if (isRecordFull(_type) || isRecordLast(_type)) {
                        _met_all = true;
                    }
                }
            }

            void reset() {
                assert(_met_all);
                _buffer.clear();
                if (_decompressor.valid()) {
                    _decompressor->reset();
                }
                _offset = kDiskNull;
                _met_all = false;
            }

        public:
            Connector(const Connector &) noexcept = delete;

            void operator=(const Connector &) noexcept = delete;

            Connector(Connector &&) noexcept = default;

            Connector & operator=(Connector &&) noexcept = default;
        };

        class NormalRecordIterator : public KeyValueIterator {
        private:
            mutable Connector _connector;
            uint32_t _k_len = 0;
            uint8_t _k_from = 0;
            bool _done = true;

        public:
            explicit NormalRecordIterator(Connector && connector) noexcept
                    : _connector(std::move(connector)) {}

            ~NormalRecordIterator() noexcept override = default;

        public:
            bool valid() const override { return !_done; };

            void seekToFirst() override {
                if (_k_len == 0) {
                    assert(_k_from == 0);
                    while (true) {
                        _connector.ensureLoad(++_k_from);
                        if (decodeVarint32(reinterpret_cast<const char *>(_connector._buffer.data()),
                                           reinterpret_cast<const char *>(_connector._buffer.data() + _k_from),
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
                _done = SliceComparator{}(key(), target);
            };

            void seekForPrev(const Slice & target) override { seek(target); }

            void next() override {
                _done = true;
            };

            void prev() override {
                next();
            };

            Slice key() const override {
                _connector.ensureLoad(_k_from + _k_len);
                return {&_connector._buffer[_k_from], _k_len};
            };

            std::pair<Slice, Meta> value() const override {
                while (!_connector._met_all) {
                    _connector.ensureLoad(static_cast<uint32_t>(_connector._buffer.size() + 1));
                }
                uint32_t start = _k_from + _k_len;
                return {{&_connector._buffer[start], _connector._buffer.size() - start},
                        Meta{false, isRecordDel(_connector._type)}};
            };

            Connector yieldConnector() override {
                return std::move(_connector);
            }
        };

        class CompressedRecordsIterator : public KeyValueIterator {
        private:
            using from_to = std::pair<uint32_t, uint32_t>;
            using kv_pair = std::pair<from_to, from_to>;

            mutable Connector _connector;
            std::vector<kv_pair> _rep;
            std::vector<kv_pair>::const_iterator _cursor;

        public:
            explicit CompressedRecordsIterator(Connector && connector) noexcept
                    : _connector(std::move(connector)), _cursor(_rep.cend()) {}

            ~CompressedRecordsIterator() noexcept override = default;

        public:
            bool valid() const override { return _cursor != _rep.cend(); };

            void seekToFirst() override {
                if (_rep.empty()) {
                    uint16_t meta_len;
                    _connector.ensureLoad(sizeof(meta_len));
                    memcpy(&meta_len, &_connector._buffer[0], sizeof(meta_len));

                    uint32_t offset = sizeof(meta_len) + meta_len;
                    _connector.ensureLoad(offset);
                    std::vector<from_to> ranges;
                    const auto * p = reinterpret_cast<const char *>(&_connector._buffer[sizeof(meta_len)]);
                    const auto * limit = p + meta_len;

                    while (p != limit) {
                        uint32_t val;
                        p = decodeVarint32(p, limit, &val);
                        if (p == nullptr) {
                            throw Exception::corruptionException("meta area of CompressedRecords broken");
                        }
                        ranges.emplace_back(offset, offset + val);
                        offset += val;
                    }
                    size_t half = ranges.size() / 2;
                    for (size_t i = 0; i < half; ++i) {
                        _rep.emplace_back(ranges[i], ranges[i + half]);
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
                while (valid() && SliceComparator()(key(), target)) {
                    next();
                }
            };

            void seekForPrev(const Slice & target) override { seek(target); }

            void next() override {
                assert(valid());
                ++_cursor;
            };

            void prev() override {
                assert(valid());
                if (_cursor == _rep.cbegin()) {
                    _cursor = _rep.cend();
                } else {
                    --_cursor;
                }
            };

            Slice key() const override {
                assert(valid());
                const auto kv = *_cursor;
                const auto k_from_to = kv.first;
                _connector.ensureLoad(k_from_to.second);
                return {&_connector._buffer[k_from_to.first], k_from_to.second - k_from_to.first};
            };

            std::pair<Slice, Meta> value() const override {
                assert(valid());
                const auto kv = *_cursor;
                const auto v_from_to = kv.second;
                _connector.ensureLoad(v_from_to.second);
                return {{&_connector._buffer[v_from_to.first], v_from_to.second - v_from_to.first},
                        Meta{true, false}};
            };

            Connector yieldConnector() override {
                return std::move(_connector);
            }
        };

        class TableIterator : public KeyOffsetSimpleIterator {
        private:
            RandomAccessFile * _data_file;
            std::unique_ptr<KeyValueIterator> _kv_iter;
            uint32_t _cursor = 1;
            uint32_t _offset{};
            bool _did_seek = false;

            friend class RecoveryIterator;

        public:
            explicit TableIterator(RandomAccessFile * data_file) noexcept
                    : _data_file(data_file) {}

            ~TableIterator() noexcept override = default;

        public:
            bool valid() const override {
                return _kv_iter->valid();
            }

            void prepare() override {
                if (!_did_seek) {
                    Connector connector(std::make_unique<RawIteratorCheckBatch>(_data_file, _cursor));
                    connector.ensureLoad(1);
                    _offset = connector._offset;
                    if (isRecordCompress(connector._type)) {
                        _kv_iter = std::make_unique<CompressedRecordsIterator>(std::move(connector));
                    } else {
                        _kv_iter = std::make_unique<NormalRecordIterator>(std::move(connector));
                    }
                }
            }

            void next() override {
                if (_did_seek) {
                    _kv_iter->next();
                } else {
                    _kv_iter->seekToFirst();
                    _did_seek = true;
                }

                if (!_kv_iter->valid()) {
                    Connector connector(_kv_iter->yieldConnector());
                    connector.reset();
                    connector.load(1);
                    if (connector._buffer.empty()) {
                        return;
                    }

                    _offset = connector._offset;
                    if (isRecordCompress(connector._type)) {
                        _kv_iter = std::make_unique<CompressedRecordsIterator>(std::move(connector));
                    } else {
                        _kv_iter = std::make_unique<NormalRecordIterator>(std::move(connector));
                    }
                    _kv_iter->seekToFirst();
                }
            }

            std::pair<Slice, std::pair<uint32_t, Meta>>
            item() const override {
                assert(valid());
                return {_kv_iter->key(), std::make_pair(_offset, _kv_iter->value().second)};
            }
        };

        class RecoveryIterator : public KeyValueSimpleIterator {
        private:
            TableIterator _table_iter;
            std::function<void(const Exception &, uint32_t)> _reporter;
            std::pair<Slice, std::pair<Slice, Meta>> _item;

        public:
            explicit RecoveryIterator(RandomAccessFile * data_file,
                                      std::function<void(const Exception &, uint32_t)> reporter) noexcept
                    : _table_iter(data_file), _reporter(std::move(reporter)) {}

            ~RecoveryIterator() noexcept override = default;

        public:
            bool valid() const override {
                return _item.first.size() != 0;
            }

            void prepare() override {}

            void next() override {
                try {
                    _table_iter.prepare();
                    _table_iter.next();
                    if (_table_iter.valid()) {
                        _item = {_table_iter._kv_iter->key(), _table_iter._kv_iter->value()};
                    } else {
                        _item = {};
                    }
                } catch (const Exception & e) {
                    _reporter(e, _table_iter._offset);

                    uint32_t cursor = _table_iter._offset;
                    restart:
                    cursor += (kLogBlockSize - cursor % kLogBlockSize);
                    try {
                        // resync
                        RawIteratorNoTypeCheck raw(_table_iter._data_file, cursor);
                        raw.prepare();
                        while (true) {
                            raw.next();
                            if (!raw.valid()) {
                                _item = {};
                                break;
                            }

                            char type = raw.item().second.second;
                            if (isRecordFull(type) || isRecordFirst(type)) {
                                if (isBatchFull(type) || isBatchFirst(type)) {
                                    // success
                                    _table_iter._cursor = raw.item().second.first;
                                    _table_iter._did_seek = false;
                                    _table_iter.prepare();
                                    _table_iter.next();
                                    assert(_table_iter.valid());
                                    _item = {_table_iter._kv_iter->key(), _table_iter._kv_iter->value()};
                                    break;
                                }
                            }
                        }
                    } catch (const Exception & ex) {
                        _reporter(ex, cursor);

                        if (!ex.isIOError()) {
                            goto restart;
                        }
                        _item = {};
                    }
                }
            }

            std::pair<Slice, std::pair<Slice, Meta>>
            item() const override {
                return _item;
            };
        };

        std::unique_ptr<Iterator<Slice/* K */, std::pair<Slice/* V */, Meta>>>
        makeRecordIterator(RandomAccessFile * data_file, uint32_t offset) {
            Connector connector(std::make_unique<RawIteratorCheckOnFly>(data_file, offset));
            connector.ensureLoad(1);
            if (isRecordCompress(connector.immut_type())) {
                return std::make_unique<CompressedRecordsIterator>(std::move(connector));
            }
            return std::make_unique<NormalRecordIterator>(std::move(connector));
        }

        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::pair<uint32_t/* offset */, Meta>>>>
        makeTableIterator(RandomAccessFile * data_file) noexcept {
            return std::make_unique<TableIterator>(data_file);
        }

        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::pair<Slice/* V */, Meta>>>>
        makeRecoveryIterator(RandomAccessFile * data_file,
                             std::function<void(const Exception &, uint32_t)> reporter) noexcept {
            return std::make_unique<RecoveryIterator>(data_file, std::move(reporter));
        }

        bool isRecordDeleted(RandomAccessFile * data_file, uint32_t offset) {
            char type;
            data_file->read(offset + 4/* checksum */, 1, &type);
            return isRecordDel(type);
        }
    }
}