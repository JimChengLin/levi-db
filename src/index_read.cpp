#include "index_internal.h"
#include "index_read.h"
#include "log_reader.h"

namespace levidb8 {
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

    BitDegradeTreeReadLog::BitDegradeTreeReadLog(const std::string & fname, RandomAccessFile * data_file)
            : BitDegradeTree(fname), _data_file(data_file) {}

    BitDegradeTreeReadLog::BitDegradeTreeReadLog(const std::string & fname, OffsetToEmpty empty,
                                                 RandomAccessFile * data_file)
            : BitDegradeTree(fname, empty), _data_file(data_file) {}

    std::pair<std::string/* res */, bool/* success */>
    BitDegradeTreeReadLog::find(const Slice & k) const {
        OffsetToData data = BitDegradeTree::find(k);
        if (data.val != kDiskNull) {
            auto iter = log_reader::makeRecordIterator(_data_file, data.val);
            iter->seek(k);
            auto value = iter->value();
            return {value.first.toString(), !value.second.del && value.first == k};
        }
        return {{}, false};
    }

    class BitDegradeTreeIteratorImpl : public KVI {
    private:
        RandomAccessFile * _data_file;
        std::unique_ptr<Iterator<Slice/* usr */, OffsetToData>> _index_iter;
        // lazy eval
        mutable std::unique_ptr<Iterator<Slice, std::pair<Slice, log_reader::Meta>>> _record_iter;
        mutable uint32_t _record_offset{};

    public:
        BitDegradeTreeIteratorImpl(RandomAccessFile * data_file,
                                   std::unique_ptr<Iterator<Slice, OffsetToData>> && index_iter) noexcept
                : _data_file(data_file), _index_iter(std::move(index_iter)) {}

        ~BitDegradeTreeIteratorImpl() noexcept override = default;

    public:
        bool valid() const override {
            return _index_iter->valid();
        }

        void seekToFirst() override {
            _index_iter->seekToFirst();
            moveToValidNext();
        }

        void seekToLast() override {
            _index_iter->seekToLast();
            moveToValidPrev();
        }

        void seek(const Slice & target) override {
            _index_iter->seek(target);
            moveToValidNext();
        }

        void seekForPrev(const Slice & target) override {
            _index_iter->seekForPrev(target);
            moveToValidPrev();
        }

        void next() override {
            _index_iter->next();
            moveToValidNext();
        }

        void prev() override {
            _index_iter->prev();
            moveToValidPrev();
        }

        Slice key() const override {
            if (_record_offset == _index_iter->value().val && _record_iter != nullptr) {
                for (_record_iter->seekToFirst();
                     _record_iter->valid();
                     _record_iter->next()) {
                    if (compatible(_record_iter->key(), _index_iter->key())) {
                        return _record_iter->key();
                    }
                }
                assert(false);
                return {};
            }
            _record_offset = _index_iter->value().val;
            _record_iter = levidb8::log_reader::makeRecordIterator(_data_file, _record_offset);
            return key();
        }

        Slice value() const override {
            if (_record_offset == _index_iter->value().val && _record_iter != nullptr) {
                for (_record_iter->seekToFirst();
                     _record_iter->valid();
                     _record_iter->next()) {
                    if (compatible(_record_iter->key(), _index_iter->key())) {
                        assert(!_record_iter->value().second.del);
                        return _record_iter->value().first;
                    }
                }
                assert(false);
                return {};
            }
            _record_offset = _index_iter->value().val;
            _record_iter = levidb8::log_reader::makeRecordIterator(_data_file, _record_offset);
            return value();
        }

    private:
        void moveToValidNext() {
            if (valid() && log_reader::isRecordDeleted(_data_file, _index_iter->value().val)) {
                next();
            }
        }

        void moveToValidPrev() {
            if (valid() && log_reader::isRecordDeleted(_data_file, _index_iter->value().val)) {
                prev();
            }
        }
    };

    std::unique_ptr<KVI>
    BitDegradeTreeReadLog::scan() const noexcept {
        return std::make_unique<BitDegradeTreeIteratorImpl>(_data_file, BitDegradeTree::scan());
    }

    class MatcherOffsetImpl : public Matcher {
    private:
        std::unique_ptr<Iterator<Slice, std::pair<Slice, log_reader::Meta>>> _iter;

    public:
        MatcherOffsetImpl(RandomAccessFile * data_file, OffsetToData data) noexcept
                : _iter(log_reader::makeRecordIterator(data_file, data.val)) {}

        ~MatcherOffsetImpl() noexcept override = default;

        // precise search
        bool operator==(const Slice & another) const override {
            _iter->seek(another);
            return _iter->key() == another;
        }

        // fuzzy search
        Slice toSlice(const Slice & target) const override {
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

        // 0 = compress
        size_t size() const override {
            if (!_iter->valid()) {
                _iter->seekToFirst();
            }
            return static_cast<size_t>(!_iter->value().second.compress);
        }

#define STR(x) #x

        [[noreturn]] bool operator==(const Matcher &) const override {
            throw Exception::notSupportedException(__FILE__ "-" STR(__LINE__));
        }

        [[noreturn]] char operator[](size_t) const override {
            throw Exception::notSupportedException(__FILE__ "-" STR(__LINE__));
        }

        [[noreturn]] Slice toSlice() const override {
            throw Exception::notSupportedException(__FILE__ "-" STR(__LINE__));
        }
    };

    std::unique_ptr<Matcher>
    BitDegradeTreeReadLog::offToMatcher(OffsetToData data) const noexcept {
        return std::make_unique<MatcherOffsetImpl>(_data_file, data);
    }

    class MatcherSliceImpl : public Matcher {
    private:
        Slice _slice;

    public:
        explicit MatcherSliceImpl(Slice slice) noexcept : _slice(std::move(slice)) {}

        ~MatcherSliceImpl() noexcept override = default;

        char operator[](size_t idx) const override {
            if (idx < _slice.size()) {
                return _slice[idx];
            }
            auto val = static_cast<uint32_t>(_slice.size());
            return reinterpret_cast<char *>(&val)[idx - _slice.size()];
        };

        bool operator==(const Matcher & another) const override {
            size_t num = size();
            if (num == another.size()) {
                for (size_t i = 0; i < num; ++i) {
                    if (operator[](i) != another[i]) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        };

        bool operator==(const Slice & another) const override {
            return _slice == another;
        };

        size_t size() const override {
            return _slice.size() + sizeof(uint32_t);
        };

        Slice toSlice() const override {
            return _slice;
        }
    };

    std::unique_ptr<Matcher> BitDegradeTreeReadLog::sliceToMatcher(const Slice & slice) const noexcept {
        return std::make_unique<MatcherSliceImpl>(slice);
    }
}