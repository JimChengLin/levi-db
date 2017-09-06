#ifndef __clang__
#include <algorithm>
#endif

#include "exception.h"
#include "index_mvcc_rd.h"
#include "log_reader.h"

namespace LeviDB {
    OffsetToData IndexMVCC::find(const Slice & k, uint64_t seq_num) const {
        if (seq_num == 0) {
            seq_num = UINT64_MAX;
        }
        OffsetToData offset{IndexConst::disk_null_};

        // 先查询内存中的索引
        auto counterpart = pendingPart(seq_num);
        counterpart->seek(k);
        if (counterpart->valid() && counterpart->key() == k) {
            offset = counterpart->value().val == IndexConst::del_marker_ ? OffsetToData{IndexConst::disk_null_}
                                                                         : counterpart->value();
        } else { // 再查 mmap
            offset = BitDegradeTree::find(k);
        }
        return offset;
    }

    void IndexMVCC::insert(const Slice & k, OffsetToData v) {
        if (_seq_gen->empty()) { // 没有快照
            BitDegradeTree::insert(k, v);
        } else {
            if (!_pending.empty() && _seq_gen->newest() == _pending.back().first) { // 已有相应 bundle
            } else {
                assert(_pending.empty() || _seq_gen->newest() > _pending.back().first);
                _pending.emplace_back(_seq_gen->newest(), std::make_shared<history_type>());
            }
            (*_pending.back().second)[k.toString()] = v;
        }
    }

    void IndexMVCC::remove(const Slice & k) {
        if (_seq_gen->empty()) {
            BitDegradeTree::remove(k);
        } else {
            if (!_pending.empty() && _seq_gen->newest() == _pending.back().first) {
            } else {
                assert(_pending.empty() || _seq_gen->newest() > _pending.back().first);
                _pending.emplace_back(_seq_gen->newest(), std::make_shared<history_type>());
            }
            (*_pending.back().second)[k.toString()] = OffsetToData{IndexConst::del_marker_};
        }
    }

    bool IndexMVCC::sync() {
        if (_seq_gen->empty()) {
            BitDegradeTree::sync();
            return true;
        }
        return false;
    }

    // 将多个 history 虚拟合并成单一 iterator
    class MultiHistoryIterator : public Iterator<Slice, OffsetToData> {
    private:
        typedef std::pair<const IndexMVCC::history, IndexMVCC::history_type::const_iterator/* state */> seq_iter;

        std::vector<seq_iter> _history_q;
        seq_iter * _cursor = nullptr;

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        MultiHistoryIterator() noexcept = default;

        MultiHistoryIterator(const MultiHistoryIterator &) = default;

        MultiHistoryIterator & operator=(const MultiHistoryIterator &) = default;

        DELETE_MOVE(MultiHistoryIterator);

        void addHistory(const IndexMVCC::history & history) noexcept {
            assert(!history->empty());
            _history_q.emplace_back(history, history->cbegin());
        }

    public:
        ~MultiHistoryIterator() noexcept override = default;

        bool valid() const override { return _cursor != nullptr; };

        void seekToFirst() override {
            for (seq_iter & iter:_history_q) {
                iter.second = iter.first->cbegin();
            }
            findSmallest();
            _direction = FORWARD;
        };

        void seekToLast() override {
            for (seq_iter & iter:_history_q) {
                iter.second = --iter.first->cend();
            }
            findLargest();
            _direction = REVERSE;
        };

        void seek(const Slice & target) override {
            for (seq_iter & iter:_history_q) {
                iter.second = iter.first->lower_bound(target);
            }
            findSmallest();
            _direction = FORWARD;
        };

        void next() override {
            assert(valid());

            if (_direction != FORWARD) {
                for (seq_iter & iter:_history_q) {
                    if (&iter != _cursor) {
                        iter.second = iter.first->upper_bound(key());
                    }
                }
                _direction = FORWARD;
            }

            ++_cursor->second;
            findSmallest();
        };

        void prev() override {
            assert(valid());

            if (_direction != REVERSE) {
                for (seq_iter & iter:_history_q) {
                    if (&iter != _cursor) {
                        iter.second = iter.first->lower_bound(key());
                        if (iter.second != iter.first->cend()) {
                            --iter.second;
                        } else {
                            iter.second = --iter.first->cend();
                        }
                    }
                }
                _direction = REVERSE;
            }

            --_cursor->second;
            findLargest();
        };

        Slice key() const override {
            assert(valid());
            return _cursor->second->first;
        };

        OffsetToData value() const override {
            assert(valid());
            return _cursor->second->second;
        };

    private:
        void findSmallest() noexcept {
            if (_history_q.empty()) {
                _cursor = nullptr;
                return;
            }

            auto smallest = (std::min_element(_history_q.crbegin(), _history_q.crend(), [](const seq_iter & a,
                                                                                           const seq_iter & b) noexcept {
                if (a.second == a.first->cend()) { // a is the biggest => !(a < b)
                    return false;
                }
                if (b.second == b.first->cend()) { // b is the biggest => a < b
                    return true;
                }
                return SliceComparator{}(a.second->first, b.second->first);
            }) + 1).base();

            if (smallest == _history_q.cend() || smallest->second == smallest->first->cend()) {
                _cursor = nullptr;
            } else {
                _cursor = &_history_q[smallest - _history_q.cbegin()];
            };
        }

        void findLargest() noexcept {
            if (_history_q.empty()) {
                _cursor = nullptr;
                return;
            }

            auto largest = (std::max_element(_history_q.crbegin(), _history_q.crend(), [](const seq_iter & a,
                                                                                          const seq_iter & b) noexcept {
                if (a.second == a.first->cend()) { // a is the smallest => a < b
                    return true;
                }
                if (b.second == b.first->cend()) { // b is the smallest => !(a < b)
                    return false;
                }
                return SliceComparator{}(a.second->first, b.second->first);
            }) + 1).base();

            if (largest == _history_q.cend() || largest->second == largest->first->cend()) {
                _cursor = nullptr;
            } else {
                _cursor = &_history_q[largest - _history_q.cbegin()];
            };
        }
    };

    std::unique_ptr<Iterator<Slice, OffsetToData>>
    IndexMVCC::pendingPart(uint64_t seq_num) const noexcept {
        auto iter = new MultiHistoryIterator;
        for (const bundle & b:_pending) {
            if (b.first < seq_num) {
                iter->addHistory(b.second);
            }
        }
        return std::unique_ptr<Iterator<Slice, OffsetToData>>(iter);
    }

    void IndexMVCC::tryApplyPending() {
        history_type merged_history;
        while (!_pending.empty()) {
            const bundle & b = _pending.front();
            if (_seq_gen->empty() || b.first < _seq_gen->oldest()) {
                for (const auto & kv:(*b.second)) merged_history[kv.first] = kv.second;
                _pending.pop_front();
            } else {
                break;
            }
        }
        for (const auto & kv:merged_history) {
            if (kv.second.val == IndexConst::del_marker_) { // 删除
                BitDegradeTree::remove(kv.first);
            } else { // 增改
                BitDegradeTree::insert(kv.first, kv.second);
            }
        }
    }

    class MatcherSliceImpl : public Matcher {
    private:
        Slice _slice;

    public:
        explicit MatcherSliceImpl(const Slice & slice) noexcept : _slice(slice) {}

        DEFAULT_MOVE(MatcherSliceImpl);
        DELETE_COPY(MatcherSliceImpl);

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
            return operator==(MatcherSliceImpl(another));
        };

        size_t size() const override {
            return _slice.size() + sizeof(uint32_t);
        };

        std::string toString() const override {
            return _slice.toString();
        };
    };

    std::unique_ptr<Matcher> IndexRead::sliceToMatcher(const Slice & slice) const noexcept {
        return std::make_unique<MatcherSliceImpl>(slice);
    }

    // 读取数据文件的实现
    class MatcherOffsetImpl : public Matcher {
    private:
        std::unique_ptr<Iterator<Slice, std::string>> _iter;
        std::exception_ptr _e;

    public:
        MatcherOffsetImpl(RandomAccessFile * data_file, OffsetToData data) noexcept {
            try { // other interfaces require noexcept, so defer it
                _iter = LogReader::makeIterator(data_file, data.val);
            } catch (const std::exception & e) {
                _e = std::current_exception();
            }
        }

        DEFAULT_MOVE(MatcherOffsetImpl);
        DELETE_COPY(MatcherOffsetImpl);

        ~MatcherOffsetImpl() noexcept override = default;

        bool operator==(const Slice & another) const override {
            if (_e) { std::rethrow_exception(_e); }
            _iter->seek(another);
            return _iter->valid() && _iter->key() == another;
        };

        std::string toString(const Slice & target) const override {
            if (_e) { std::rethrow_exception(_e); }
            _iter->seek(target);
            if (_iter->valid()) {
                return _iter->key().toString();
            }
            // 尽量返回最接近的值
            _iter->seekToLast();
            if (_iter->valid()) {
                return _iter->key().toString();
            }
            return {};
        };

        std::string getValue(const Slice & target) const override {
            if (_e) { std::rethrow_exception(_e); }
            _iter->seek(target);
            if (_iter->valid() && _iter->key() == target) {
                return _iter->value();
            }
            return {};
        };

        // dirty hack, 原是无用的接口方法, 现用来表示是否压缩, 0 = 是, 1 = 否
        size_t size() const override {
            return static_cast<size_t>(!LogReader::isRecordIteratorCompress(_iter.get()));
        };

        // 以下方法在 BitDegradeTree 中没有用到, 所以不强行实现
        [[noreturn]] bool operator==(const Matcher & another) const override {
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] char operator[](size_t idx) const override {
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };

        [[noreturn]] std::string toString() const override {
            throw Exception::notSupportedException(__FILE__ "-" LEVI_STR(__LINE__));
        };
    };

    std::unique_ptr<Matcher> IndexRead::offToMatcher(OffsetToData data) const noexcept {
        return std::make_unique<MatcherOffsetImpl>(_data_file, data);
    }

    std::pair<std::string/* res */, bool/* success */>
    IndexRead::find(const Slice & k, uint64_t seq_num) const {
        OffsetToData data = IndexMVCC::find(k, seq_num);
        if (data.val != IndexConst::disk_null_) {
            std::unique_ptr<Matcher> m = offToMatcher(data);
            if (*m == k) {
                std::string res = m->getValue(k);
                // del 在 last char
                auto del = static_cast<bool>(res.back());
                res.pop_back();
                return {std::move(res), !del};
            }
        }
        return {{}, false};
    }
}