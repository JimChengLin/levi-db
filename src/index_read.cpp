#include "index_read.h"

namespace levidb8 {
    inline static uint32_t unmaskOffsetToData(OffsetToData data) noexcept {
        return data.val & (~(1 << 31));
    }

    static bool compatible(const Slice & key, const Slice & target) noexcept {
        for (size_t i = 0; i < target.size(); ++i) {
            char k = 0;
            char t = target[i];
            if (i < key.size()) {
                k = key[i];
            } else if (i - key.size() < sizeof(uint32_t)) {
                auto length = static_cast<uint32_t>(key.size());
                k = reinterpret_cast<const char *>(&length)[i - key.size()];
            }
            if ((k & t) == t) {
            } else {
                return false;
            }
        }
        return true;
    }

    MatcherOffsetImpl::MatcherOffsetImpl(OffsetToData data, CacheImpl & cache)
            : _iter(RecordIterator::open(cache.data_file, unmaskOffsetToData(data), cache.record_cache)) {}

    // precise search
    bool MatcherOffsetImpl::operator==(const Slice & another) const {
        _iter->seek(another);
        return _iter->key() == another;
    }

    // fuzzy search
    Slice MatcherOffsetImpl::toSlice(const USR & usr) const {
        Slice target = usr.toSlice();
        for (_iter->seekToFirst();
             _iter->valid();
             _iter->next()) {
            if (compatible(_iter->key(), target)) {
                return _iter->key();
            }
        }
        _iter->seekToLast();
        return _iter->key();
    }

    bool MatcherOffsetImpl::isCompress() const {
        return _iter->info().compress;
    }

    MatcherSliceImpl::MatcherSliceImpl(Slice slice) noexcept : _slice(std::move(slice)) {}

    char MatcherSliceImpl::operator[](size_t idx) const noexcept {
        if (idx < _slice.size()) {
            return _slice[idx];
        }
        auto val = static_cast<uint32_t>(_slice.size());
        return reinterpret_cast<const char *>(&val)[idx - _slice.size()];
    }

    size_t MatcherSliceImpl::size() const noexcept {
        return _slice.size() + sizeof(uint32_t);
    }

    BitDegradeTreeRead::BitDegradeTreeRead(const std::string & fname, RandomAccessFile * data_file)
            : BitDegradeTree(fname) {
        _cache.data_file = data_file;
    }

    BitDegradeTreeRead::BitDegradeTreeRead(const std::string & fname, OffsetToEmpty empty, RandomAccessFile * data_file)
            : BitDegradeTree(fname, empty) {
        _cache.data_file = data_file;
    }

    bool BitDegradeTreeRead::find(const Slice & k, std::string * value) const {
        OffsetToData data = BitDegradeTree::find(k);
        if (data.val != kDiskNull) {
            auto iter = RecordIterator::open(_cache.data_file, unmaskOffsetToData(data), _cache.record_cache);
            if (iter->info().del) {
                return false;
            }
            iter->seek(k);
            if (iter->key() == k) {
                if (value != nullptr) {
                    Slice v = iter->value();
                    value->assign(v.data(), v.size());
                }
                return true;
            }
        }
        return false;
    }

    class BDIteratorRead : public Iterator<Slice, Slice, bool> {
    private:
        CacheImpl * _cache;
        BitDegradeTreeRead::BDIterator _iter;
        // lazy init
        mutable std::unique_ptr<RecordIterator> _record_iter;
        mutable uint32_t _record_offset{};

    public:
        explicit BDIteratorRead(CacheImpl * cache, const BitDegradeTreeRead * index) noexcept
                : _cache(cache), _iter(index) {}

        ~BDIteratorRead() noexcept override = default;

    public:
        bool valid() const override {
            return _iter.valid();
        }

        void seekToFirst() override {
            _iter.seekToFirst();
        }

        void seekToLast() override {
            _iter.seekToLast();
        }

        void seek(const Slice & target) override {
            _iter.seek(target);
            ensureLargerOrEqual(target);
        }

        void next() override {
            _iter.next();
        }

        void prev() override {
            _iter.prev();
        }

        Slice key() const override {
            if (not(_record_offset == _iter.value().val && _record_iter != nullptr)) {
                _record_offset = unmaskOffsetToData(_iter.value());
                _record_iter = RecordIterator::open(_cache->data_file, _record_offset, _cache->record_cache);
            } else if (_record_iter->valid() && compatible(_record_iter->key(), _iter.key())) {
                return _record_iter->key();
            }

            for (_record_iter->seekToFirst();
                 _record_iter->valid();
                 _record_iter->next()) {
                if (compatible(_record_iter->key(), _iter.key())) {
                    return _record_iter->key();
                }
            }
            assert(false);
            return {};
        }

        Slice value() const override {
            if (not(_record_offset == _iter.value().val && _record_iter != nullptr)) {
                _record_offset = unmaskOffsetToData(_iter.value());
                _record_iter = RecordIterator::open(_cache->data_file, _record_offset, _cache->record_cache);
            } else if (_record_iter->valid() && compatible(_record_iter->key(), _iter.key())) {
                return _record_iter->value();
            }

            for (_record_iter->seekToFirst();
                 _record_iter->valid();
                 _record_iter->next()) {
                if (compatible(_record_iter->key(), _iter.key())) {
                    return _record_iter->value();
                }
            }
            assert(false);
            return {};
        }

    private:
        void ensureLargerOrEqual(const Slice & target) {
            while (valid() && SliceComparator()(key(), target)) {
                next();
            }
        }
    };

    std::unique_ptr<Iterator<Slice, Slice, bool>>
    BitDegradeTreeRead::scan() const noexcept {
        return std::make_unique<BDIteratorRead>(&_cache, this);
    }
}