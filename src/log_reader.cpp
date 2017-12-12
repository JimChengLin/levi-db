#include "log_reader.h"
#include "compress.h"
#include "crc32c.h"
#include "env_io.h"
#include "varint.h"

namespace levidb8 {
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

    static inline bool isBatchFull(char t) noexcept { return (t & 0b11) == 0b00; }

    static inline bool isBatchFirst(char t) noexcept { return (t & 0b11) == 0b01; }

    static inline bool isBatchMiddle(char t) noexcept { return (t & 0b11) == 0b10; }

    static inline bool isBatchLast(char t) noexcept { return (t & 0b11) == 0b11; }

    class RawIterator
            : public SimpleIterator
                    <std::pair<Slice /* page */, std::pair<uint32_t /* offset */, char /* type */>>> {
    public:
        virtual void restart(RandomAccessFile * data_file, uint32_t offset) = 0;
    };

    class Connector;

    class KeyValueIterator
            : public Iterator<Slice /* K */, Slice /* V */, LogMeta> {
    public:
        virtual Connector yieldConnector() = 0;

        virtual void restart(Connector && connector) = 0;

        virtual bool isStatic() const = 0;
    };

    class RawIteratorNoTypeCheck : public RawIterator {
    private:
        RandomAccessFile * _dst;
        uint32_t _cursor;
        Slice _page;
        char _backing_store[kLogBlockSize];
        char _type = 0; // FULL
        bool _eof = false;

    public:
        RawIteratorNoTypeCheck(RandomAccessFile * dst, uint32_t offset) noexcept
                : _dst(dst), _cursor(offset) {}

        ~RawIteratorNoTypeCheck() noexcept override = default;

        bool valid() const override { return !_eof; }

        void next() override {
            restart:
            bool pad = (_cursor % kPageSize == 0
                        && (isRecordLast(_type) || isRecordFull(_type)) && _page.size() != 0);
            _cursor += static_cast<uint32_t>(pad);
            size_t block_offset = _cursor % kLogBlockSize;
            size_t remaining_bytes = kLogBlockSize - block_offset;

            if (remaining_bytes < kLogHeaderSize) {
                _cursor += remaining_bytes;
                goto restart;
            }

            char buf[kLogHeaderSize];
            if (_dst->read(_cursor, kLogHeaderSize, buf).size() != kLogHeaderSize) {
                _eof = true;
                return;
            }
            _cursor += kLogHeaderSize;
            remaining_bytes -= kLogHeaderSize;

            uint16_t length;
            memcpy(&length, buf /* start */ + 4 /* checksum */ + 1 /* type */,
                   sizeof(length));
            if (length > remaining_bytes) {
                throw Exception::corruptionException("bad record length");
            }

            if (_dst->read(_cursor, length, _backing_store).size() != length) {
                _eof = true;
                return;
            };
            _cursor += length;

            uint32_t calc_checksum = crc32c::extend(
                    crc32c::value(buf + 4, 1 + sizeof(length)), _backing_store, length);
            uint32_t store_checksum;
            memcpy(&store_checksum, buf, sizeof(store_checksum));
            if (calc_checksum != store_checksum) {
                throw Exception::corruptionException("log checksum mismatch");
            }

            _type = buf[4];
            _page = Slice(_backing_store, length);
        }

        std::pair<Slice, std::pair<uint32_t, char>> item() const override {
            assert(valid());
            return {_page,
                    std::make_pair(_cursor - _page.size() - kLogHeaderSize, _type)};
        }

        void restart(RandomAccessFile * data_file, uint32_t offset) override {
            _dst = data_file;
            _cursor = offset;
            _type = 0;
            _eof = false;
        }
    };

    class RawIteratorCheckOnFly : public RawIterator {
    private:
        RawIteratorNoTypeCheck _iter;
        char _prev_type = 0;

    public:
        RawIteratorCheckOnFly(RandomAccessFile * dst, uint32_t offset) noexcept
                : _iter(dst, offset) {}

        ~RawIteratorCheckOnFly() noexcept override = default;

        bool valid() const override { return _iter.valid(); }

        void next() override {
            _iter.next();
            checkedOrThrow();
        }

        std::pair<Slice, std::pair<uint32_t, char>> item() const override {
            return _iter.item();
        }

        void restart(RandomAccessFile * data_file, uint32_t offset) override {
            _iter.restart(data_file, offset);
            _prev_type = 0;
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
            if (isRecordFirst(type_a) ||
                isRecordMiddle(type_a)) { // prev is starting
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
        std::vector<std::pair<std::vector<uint8_t>, std::pair<uint32_t, char>>>::
        const_iterator _buffer_cursor;
        char _prev_type = 0;

    public:
        RawIteratorCheckBatch(RandomAccessFile * dst, uint32_t offset) noexcept
                : _iter(dst, offset), _buffer_cursor(_buffer.cend()) {}

        ~RawIteratorCheckBatch() noexcept override = default;

        bool valid() const override { return _buffer_cursor != _buffer.cend(); }

        void next() override {
            if (_buffer_cursor == _buffer.cend() ||
                ++_buffer_cursor == _buffer.cend()) {
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

                    _buffer.emplace_back(
                            std::vector<uint8_t>(
                                    reinterpret_cast<const uint8_t *>(page.data()),
                                    reinterpret_cast<const uint8_t *>(page.data() +
                                                                      page.size())),
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
            return {_buffer_cursor->first, _buffer_cursor->second};
        }

        void restart(RandomAccessFile * data_file, uint32_t offset) override {
            _iter.restart(data_file, offset);
            _buffer.clear();
            _buffer_cursor = _buffer.cend();
            _prev_type = 0;
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

        bool valid() const override { return true; }

        void next() override {}

        Slice item() const override { return *_src; }
    };

    class Decompressor {
    private:
        Slice _src;
        compress::Decoder _decode_iter;

    public:
        Decompressor() : _decode_iter(std::make_unique<DecompressHelper>(&_src)) {}

        Slice submit(const Slice & code) {
            _src = code;
            _decode_iter.next();
            assert(_decode_iter.valid());
            return _decode_iter.item();
        }

        bool isLastPage() const noexcept { return _decode_iter.isLastPage(); }

        Slice request() {
            _decode_iter.next();
            assert(_decode_iter.valid());
            return _decode_iter.item();
        }

        void reset() { _decode_iter.reset(); }
    };

    class Connector {
    private:
        std::unique_ptr<RawIterator> _raw_iter;
        std::unique_ptr<Decompressor> _decompressor;
        std::vector<uint8_t> _buffer;

        uint32_t _offset = kDiskNull;
        char _type{}; // 仅需 compress 和 del
        bool _met_all = false;

        friend class NormalRecordIterator;

        friend class CompressedRecordsIterator;

        friend class RecordIteratorCacheNormalImpl;

        friend class RecordIteratorCacheCompressImpl;

        friend class TableIteratorImpl;

    public:
        Connector() noexcept {};

        explicit Connector(std::unique_ptr<RawIterator> && raw_iter) noexcept
                : _raw_iter(std::move(raw_iter)) {}

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

                if (isRecordCompress(_type)) {
                    if (_decompressor == nullptr) { // lazy init
                        _decompressor = std::make_unique<Decompressor>();
                    }
                    Slice content = _decompressor->submit(page);
                    while (true) {
                        _buffer.insert(
                                _buffer.end(),
                                reinterpret_cast<const uint8_t *>(content.data()),
                                reinterpret_cast<const uint8_t *>(content.data() +
                                                                  content.size()));
                        if (_decompressor->isLastPage()) {
                            break;
                        }
                        _decompressor->request();
                    }
                } else {
                    _buffer.insert(_buffer.end(),
                                   reinterpret_cast<const uint8_t *>(page.data()),
                                   reinterpret_cast<const uint8_t *>(page.data() +
                                                                     page.size()));
                }

                if (isRecordFull(_type) || isRecordLast(_type)) {
                    _met_all = true;
                }
            }
        }

        void skipToNext() {
            while (!_met_all) {
                _raw_iter->next();
                if (!_raw_iter->valid()) {
                    break;
                }
                _type = _raw_iter->item().second.second;
            }
        }

        void reset() {
            if (_decompressor != nullptr) {
                _decompressor->reset();
            }
            _buffer.clear();
            _offset = kDiskNull;
            _met_all = false;
        }

        void restart(RandomAccessFile * data_file, uint32_t offset) {
            reset();
            _raw_iter->restart(data_file, offset);
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

        friend class RecordIteratorCacheNormalImpl;

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
                    if (decodeVarint32(reinterpret_cast<const char *>(
                                               _connector._buffer.data()),
                                       reinterpret_cast<const char *>(
                                               _connector._buffer.data() + _k_from),
                                       &_k_len) != nullptr) {
                        break;
                    }
                }
            }
            _done = false;
        }

        void seekToLast() override { seekToFirst(); }

        void seek(const Slice & target) override {
            seekToFirst();
            _done = SliceComparator{}(key(), target);
        }

        void next() override { _done = true; }

        void prev() override { next(); }

        Slice key() const override {
            _connector.ensureLoad(_k_from + _k_len);
            return {&_connector._buffer[_k_from], _k_len};
        }

        Slice value() const override {
            while (!_connector._met_all) {
                _connector.ensureLoad(
                        static_cast<uint32_t>(_connector._buffer.size() + 1));
            }
            uint32_t start = _k_from + _k_len;
            return {&_connector._buffer[start], _connector._buffer.size() - start};
        }

        LogMeta info() const override {
            assert(!_connector._buffer.empty());
            return {false, isRecordDel(_connector._type)};
        }

        Connector yieldConnector() override { return std::move(_connector); }

        void restart(Connector && connector) override {
            _connector = std::move(connector);
            _k_len = 0;
            _k_from = 0;
            _done = true;
        }

        bool isStatic() const override { return _connector._met_all && _k_len != 0; }
    };

    class CompressedRecordsIterator : public KeyValueIterator {
    private:
        using from_to = std::pair<uint32_t, uint32_t>;
        using kv_pair = std::pair<from_to, from_to>;

        mutable Connector _connector;
        std::vector<kv_pair> _rep;
        std::vector<kv_pair>::const_iterator _cursor;

        friend class RecordIteratorCacheCompressImpl;

    public:
        explicit CompressedRecordsIterator(Connector && connector) noexcept
                : _connector(std::move(connector)), _cursor(_rep.cend()) {}

        ~CompressedRecordsIterator() noexcept override = default;

    public:
        bool valid() const override { return _cursor != _rep.cend(); }

        void seekToFirst() override {
            if (_rep.empty()) {
                uint16_t meta_len;
                _connector.ensureLoad(sizeof(meta_len));
                memcpy(&meta_len, &_connector._buffer[0], sizeof(meta_len));

                uint32_t offset = sizeof(meta_len) + meta_len;
                _connector.ensureLoad(offset);
                std::vector<from_to> ranges;
                const auto * p = reinterpret_cast<const char *>(
                        &_connector._buffer[sizeof(meta_len)]);
                const auto * limit = p + meta_len;

                while (p != limit) {
                    uint32_t val;
                    p = decodeVarint32(p, limit, &val);
                    if (p == nullptr) {
                        throw Exception::corruptionException(
                                "broken meta area of CompressedRecords");
                    }
                    ranges.emplace_back(offset, offset + val);
                    offset += val;
                }
                size_t half = ranges.size() / 2;
                assert(ranges.size() % 2 == 0);
                for (size_t i = 0; i < half; ++i) {
                    _rep.emplace_back(ranges[i], ranges[i + half]);
                }
            }

            _cursor = _rep.cbegin();
        }

        void seekToLast() override {
            seekToFirst();
            _cursor = --_rep.cend();
        }

        void seek(const Slice & target) override {
            seekToFirst();
            while (valid() && SliceComparator()(key(), target)) {
                next();
            }
        }

        void next() override {
            assert(valid());
            ++_cursor;
        }

        void prev() override {
            assert(valid());
            if (_cursor == _rep.cbegin()) {
                _cursor = _rep.cend();
            } else {
                --_cursor;
            }
        }

        Slice key() const override {
            assert(valid());
            const auto k_from_to = _cursor->first;
            _connector.ensureLoad(k_from_to.second);
            return {&_connector._buffer[k_from_to.first],
                    k_from_to.second - k_from_to.first};
        }

        Slice value() const override {
            assert(valid());
            const auto v_from_to = _cursor->second;
            _connector.ensureLoad(v_from_to.second);
            return {&_connector._buffer[v_from_to.first],
                    v_from_to.second - v_from_to.first};
        }

        LogMeta info() const override {
            return {true, false};
        }

        Connector yieldConnector() override { return std::move(_connector); }

        void restart(Connector && connector) override {
            _connector = std::move(connector);
            _rep.clear();
            _cursor = _rep.cend();
        }

        bool isStatic() const override { return _connector._met_all && !_rep.empty(); }
    };

    class TableIteratorImpl : public TableIterator {
    private:
        KeyValueIterator * _kv_iter{};
        NormalRecordIterator _normal_iter;
        CompressedRecordsIterator _compress_iter;
        uint32_t _offset = 1;
        bool _did_seek = false;

        friend class RecoveryIteratorImpl;

    public:
        TableIteratorImpl() noexcept : _normal_iter(Connector()), _compress_iter(Connector()) {};

        explicit TableIteratorImpl(RandomAccessFile * data_file)
                : _normal_iter(Connector()), _compress_iter(Connector()) {
            Connector connector(std::make_unique<RawIteratorCheckBatch>(data_file, _offset));
            connector.ensureLoad(1);
            if (isRecordCompress(connector._type)) {
                _compress_iter.restart(std::move(connector));
                _kv_iter = &_compress_iter;
            } else {
                _normal_iter.restart(std::move(connector));
                _kv_iter = &_normal_iter;
            }
        }

        ~TableIteratorImpl() noexcept override = default;

    public:
        bool valid() const override { return _kv_iter->valid(); }

        void next() override {
            if (_did_seek) {
                _kv_iter->next();
            } else {
                _kv_iter->seekToFirst();
                _did_seek = true;
            }

            if (!_kv_iter->valid()) {
                Connector connector(_kv_iter->yieldConnector());
                connector.skipToNext();
                connector.reset();
                connector.load(1);
                if (connector._buffer.empty()) {
                    return;
                }

                _offset = connector._offset;
                if (isRecordCompress(connector._type)) {
                    _compress_iter.restart(std::move(connector));
                    _kv_iter = &_compress_iter;
                } else {
                    _normal_iter.restart(std::move(connector));
                    _kv_iter = &_normal_iter;
                }
                _kv_iter->seekToFirst();
            }
        }

        std::pair<Slice, uint32_t> item() const override {
            assert(valid());
            return {_kv_iter->key(), _offset};
        }

        LogMeta info() const override {
            return _kv_iter->info();
        }

    public:
        void restart(RandomAccessFile * data_file, uint32_t offset) {
            Connector connector(std::make_unique<RawIteratorCheckBatch>(data_file, offset));
            connector.ensureLoad(1);
            _offset = connector._offset;
            if (isRecordCompress(connector._type)) {
                _compress_iter.restart(std::move(connector));
                _kv_iter = &_compress_iter;
            } else {
                _normal_iter.restart(std::move(connector));
                _kv_iter = &_normal_iter;
            }
            _did_seek = false;
        }
    };

    class RecoveryIteratorImpl : public RecoveryIterator {
    private:
        RandomAccessFile * const _data_file;
        TableIteratorImpl _table_iter;
        std::function<void(const Exception &, uint32_t)> _reporter;
        std::pair<Slice, Slice> _item;
        LogMeta _info;

    public:
        explicit RecoveryIteratorImpl(
                RandomAccessFile * data_file,
                std::function<void(const Exception &, uint32_t)> reporter) noexcept
                : _data_file(data_file), _reporter(std::move(reporter)) {}

        ~RecoveryIteratorImpl() noexcept override = default;

    public:
        bool valid() const override { return _item.first.size() != 0; }

        void next() override {
            try {
                if (_table_iter._kv_iter == nullptr) { // init
                    _table_iter.restart(_data_file, _table_iter._offset);
                }
                _table_iter.next();
                if (_table_iter.valid()) {
                    _item = {_table_iter._kv_iter->key(),
                             _table_iter._kv_iter->value()};
                    _info = _table_iter.info();
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
                    RawIteratorNoTypeCheck raw(_data_file, cursor);
                    while (true) {
                        raw.next();
                        if (!raw.valid()) {
                            _item = {};
                            break;
                        }

                        auto item = raw.item();
                        char type = item.second.second;
                        if (isRecordFull(type) || isRecordFirst(type)) {
                            if (isBatchFull(type) || isBatchFirst(type)) {
                                // success
                                _table_iter.restart(_data_file, item.second.first);
                                _table_iter.next();
                                assert(_table_iter.valid());
                                _item = {_table_iter._kv_iter->key(),
                                         _table_iter._kv_iter->value()};
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

        std::pair<Slice, Slice> item() const override { return _item; };

        LogMeta info() const override { return _info; }
    };

    std::unique_ptr<TableIterator>
    TableIterator::open(RandomAccessFile * data_file) {
        return std::make_unique<TableIteratorImpl>(data_file);
    }

    std::unique_ptr<RecoveryIterator> RecoveryIterator::open(
            RandomAccessFile * data_file,
            std::function<void(const Exception &, uint32_t)> reporter) noexcept {
        return std::make_unique<RecoveryIteratorImpl>(data_file, std::move(reporter));
    }

    RecordCache::~RecordCache() noexcept {
        for (auto * cache : {&compress_obj_cache, &normal_obj_cache}) {
            for (size_t i = 0; i < cache->size(); ++i) {
                delete cache->operator[](i).load(std::memory_order_acquire);
            }
        }
    }

    class RecordIteratorCacheNormalImpl : public RecordIterator {
    private:
        std::shared_ptr<RecordCache::DataUnit> _unit;
        NormalRecordIterator * _normal_iter;
        bool _done = true;

    public:
        explicit RecordIteratorCacheNormalImpl(
                std::shared_ptr<RecordCache::DataUnit> unit) noexcept
                : _unit(std::move(unit)),
                  _normal_iter(static_cast<NormalRecordIterator *>(_unit->iter.get())) {}

        ~RecordIteratorCacheNormalImpl() noexcept override = default;

    public:
        bool valid() const override { return !_done; };

        void seekToFirst() override { _done = false; }

        void seekToLast() override { seekToFirst(); }

        void seek(const Slice & target) override {
            _done = SliceComparator{}(key(), target);
        }

        void next() override { _done = true; }

        void prev() override { next(); }

        Slice key() const override {
            return {&_normal_iter->_connector._buffer[_normal_iter->_k_from],
                    _normal_iter->_k_len};
        }

        Slice value() const override {
            uint32_t start = _normal_iter->_k_from + _normal_iter->_k_len;
            return {&_normal_iter->_connector._buffer[start],
                    _normal_iter->_connector._buffer.size() - start};
        }

        LogMeta info() const override {
            return {false, isRecordDel(_normal_iter->_connector._type)};
        }
    };

    class RecordIteratorCacheCompressImpl : public RecordIterator {
    private:
        std::shared_ptr<RecordCache::DataUnit> _unit;
        CompressedRecordsIterator * _compress_iter;
        decltype(_compress_iter->_cursor) _cursor;

    public:
        explicit RecordIteratorCacheCompressImpl(
                std::shared_ptr<RecordCache::DataUnit> unit) noexcept
                : _unit(std::move(unit)),
                  _compress_iter(static_cast<CompressedRecordsIterator *>(_unit->iter.get())),
                  _cursor(_compress_iter->_rep.cend()) {}

        ~RecordIteratorCacheCompressImpl() noexcept override = default;

    public:
        bool valid() const override {
            return _cursor != _compress_iter->_rep.cend();
        }

        void seekToFirst() override { _cursor = _compress_iter->_rep.cbegin(); }

        void seekToLast() override { _cursor = --_compress_iter->_rep.cend(); }

        void seek(const Slice & target) override {
            seekToFirst();
            while (valid() && SliceComparator()(key(), target)) {
                next();
            }
        }

        void next() override { ++_cursor; }

        void prev() override {
            if (_cursor == _compress_iter->_rep.cbegin()) {
                _cursor = _compress_iter->_rep.cend();
            } else {
                --_cursor;
            }
        }

        Slice key() const override {
            const auto k_from_to = _cursor->first;
            return {&_compress_iter->_connector._buffer[k_from_to.first],
                    k_from_to.second - k_from_to.first};
        }

        Slice value() const override {
            const auto v_from_to = _cursor->second;
            return {&_compress_iter->_connector._buffer[v_from_to.first],
                    v_from_to.second - v_from_to.first};
        }

        LogMeta info() const override { return {true, false}; }
    };

    class RecordIteratorImpl : public RecordIterator {
    private:
        std::unique_ptr<KeyValueIterator> _iter;
        RecordCache * _record_cache;
        uint32_t _offset;
        bool _compress;

    public:
        RecordIteratorImpl(std::unique_ptr<KeyValueIterator> && iter,
                           RecordCache * record_cache, uint32_t offset,
                           bool compress)
                : _iter(std::move(iter)), _record_cache(record_cache), _offset(offset),
                  _compress(compress) {}

        ~RecordIteratorImpl() override {
            if (_iter->isStatic() && __builtin_popcountll(reinterpret_cast<uintptr_t>(this)) % 2 == 0) {
                size_t pos = std::hash<uint32_t>()(_offset) % _record_cache->data_cache.size();
                auto ptr = std::make_shared<RecordCache::DataUnit>();
                ptr->iter = std::move(_iter);
                ptr->offset = _offset;
                ptr->compress = _compress;
                std::atomic_store(&_record_cache->data_cache[pos], ptr);
            } else {
                for (auto & ptr : (_compress ? _record_cache->compress_obj_cache
                                             : _record_cache->normal_obj_cache)) {
                    if (ptr.load(std::memory_order_acquire) == nullptr) {
                        decltype(ptr.load()) e = nullptr;
                        if (ptr.compare_exchange_strong(e, _iter.get())) {
                            _iter.release();
                            break;
                        }
                    }
                }
            }
        }

    public:
        bool valid() const override { return _iter->valid(); }

        void seekToFirst() override { _iter->seekToFirst(); }

        void seekToLast() override { _iter->seekToLast(); }

        void seek(const Slice & target) override { _iter->seek(target); }

        void next() override { _iter->next(); }

        void prev() override { _iter->prev(); }

        Slice key() const override { return _iter->key(); }

        Slice value() const override { return _iter->value(); }

        LogMeta info() const override { return _iter->info(); }
    };

    std::unique_ptr<RecordIterator>
    RecordIterator::open(RandomAccessFile * data_file, uint32_t offset,
                         RecordCache & cache) {
        assert(offset < kLogFileLimit);
        {
            const size_t pos = std::hash<uint32_t>()(offset) % cache.data_cache.size();
            auto ptr = std::atomic_load(&cache.data_cache[pos]);
            if (ptr != nullptr && ptr->offset == offset) {
                if (ptr->compress) {
                    return std::make_unique<RecordIteratorCacheCompressImpl>(std::move(ptr));
                }
                return std::make_unique<RecordIteratorCacheNormalImpl>(std::move(ptr));
            }
        }

        char type;
        data_file->read(offset + 4 /* checksum */, 1, &type);
        const bool compress = isRecordCompress(type);
        for (auto & p :(compress ? cache.compress_obj_cache : cache.normal_obj_cache)) {
            decltype(p.load()) e;
            if ((e = p.load(std::memory_order_acquire)) != nullptr) {
                if (p.compare_exchange_strong(e, nullptr)) {
                    Connector connector = static_cast<KeyValueIterator *>(e)->yieldConnector();
                    connector.restart(data_file, offset);
                    connector.ensureLoad(1);
                    static_cast<KeyValueIterator *>(e)->restart(std::move(connector));
                    return std::make_unique<RecordIteratorImpl>(
                            std::unique_ptr<KeyValueIterator>(static_cast<KeyValueIterator *>(e)),
                            &cache, offset, compress);
                }
            }
        }

        auto connector = Connector(std::make_unique<RawIteratorCheckOnFly>(data_file, offset));
        connector.ensureLoad(1);
        if (compress) {
            return std::make_unique<RecordIteratorImpl>(
                    std::make_unique<CompressedRecordsIterator>(std::move(connector)),
                    &cache, offset, compress);
        }
        return std::make_unique<RecordIteratorImpl>(
                std::make_unique<NormalRecordIterator>(std::move(connector)),
                &cache, offset, compress);
    }
} // namespace levidb8