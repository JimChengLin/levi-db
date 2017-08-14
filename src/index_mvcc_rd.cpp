#ifndef __clang__
#include <algorithm>
#endif

#include "index_mvcc_rd.h"

namespace LeviDB {
    static constexpr uint32_t del_marker_ = IndexConst::disk_null_ - 1; // 和 disk_null_ 一样不是有效的 input

    OffsetToData IndexMVCC::find(const Slice & k, uint64_t seq_num) const {
        if (seq_num == 0) {
            seq_num = UINT64_MAX;
        }
        OffsetToData offset{IndexConst::disk_null_};

        // 先查询内存中的索引
        auto counterpart = pendingPart(seq_num);
        counterpart->seek(k);
        if (counterpart->valid() && counterpart->key() == k) {
            offset = counterpart->value();
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
                _pending.emplace_back(_seq_gen->newest(), history{});
            }
            _pending.back().second.emplace(k.toString(), v);
        }
    }

    void IndexMVCC::remove(const Slice & k) {
        if (_seq_gen->empty()) {
            BitDegradeTree::remove(k);
        } else {
            if (!_pending.empty() && _seq_gen->newest() == _pending.back().first) {
            } else {
                assert(_pending.empty() || _seq_gen->newest() > _pending.back().first);
                _pending.emplace_back(_seq_gen->newest(), history{});
            }
            _pending.back().second.emplace(k.toString(), OffsetToData{del_marker_});
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
        typedef std::pair<const IndexMVCC::history *, IndexMVCC::history::const_iterator/* state */> seq_iter;

        std::vector<seq_iter> _history_q;
        seq_iter * _cursor = nullptr;

        enum Direction {
            FORWARD,
            REVERSE,
        };
        Direction _direction = FORWARD;

    public:
        MultiHistoryIterator() noexcept = default;

        MultiHistoryIterator(const MultiHistoryIterator &) = default; // because of std::vector

        MultiHistoryIterator & operator=(const MultiHistoryIterator &) = default;

        DEFAULT_MOVE(MultiHistoryIterator);

        void addHistory(const IndexMVCC::history * history) noexcept {
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
            auto smallest = std::min_element(_history_q.cbegin(), _history_q.cend(), [](const seq_iter & a,
                                                                                        const seq_iter & b) {
                if (a.second == a.first->cend()) { // a is the biggest => !(a < b)
                    return false;
                }
                if (b.second == b.first->cend()) { // b is the biggest => a < b
                    return true;
                }
                return SliceComparator{}(a.second->first, b.second->first);
            });

            if (smallest == _history_q.cend() || smallest->second == smallest->first->cend()) {
                _cursor = nullptr;
            } else {
                _cursor = &_history_q[smallest - _history_q.cbegin()];
            };
        }

        void findLargest() noexcept {
            auto largest = std::max_element(_history_q.cbegin(), _history_q.cend(), [](const seq_iter & a,
                                                                                       const seq_iter & b) {
                if (a.second == a.first->cend()) { // a is the smallest => a < b
                    return true;
                }
                if (b.second == b.first->cend()) { // b is the smallest => !(a < b)
                    return false;
                }
                return SliceComparator{}(a.second->first, b.second->first);
            });

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
                iter->addHistory(&b.second);
            }
        }
        return std::unique_ptr<Iterator<Slice, OffsetToData>>{iter};
    }

    void IndexMVCC::tryApplyPending() {
        history merged_history;
        while (!_pending.empty()) {
            const bundle & b = _pending.front();
            if (_seq_gen->empty() || b.first < _seq_gen->oldest()) {
                for (const auto & kv:b.second) merged_history[kv.first] = kv.second;
                _pending.pop_front();
            } else {
                break;
            }
        }
        for (const auto & kv:merged_history) {
            if (kv.second.val == del_marker_) { // 删除,
                BitDegradeTree::remove(kv.first);
            } else { // 增改
                BitDegradeTree::insert(kv.first, kv.second);
            }
        }
    }

    // 真正读取数据文件的 Matcher 实现
    class MatcherOffsetImpl : public Matcher {

    };

    std::unique_ptr<Matcher> IndexRead::offToMatcher(OffsetToData data) const noexcept {

    }

    class MatcherSliceImpl : public Matcher {

    };

    std::unique_ptr<Matcher> IndexRead::sliceToMatcher(const Slice & slice) const noexcept {

    }
}