#ifndef __clang__
#include <algorithm>
#endif

#include "index_iter_regex.h"
#include "log_reader.h"
#include "merger.h"

namespace LeviDB {
    template<bool RIGHT_FIRST>
    class IndexIter::BitDegradeTreeNodeIter : public SimpleIterator<std::pair<Slice, Slice>> {
    private:
        const BDNode * _node;
        const IndexIter * _index;
        const UsrJudge * _judge;
        USR * _reveal_info;

        const uint32_t * _cbegin;
        const uint32_t * _cend;
        std::pair<Slice, std::string> _item;

        const uint32_t * min_it = nullptr;
        std::unique_ptr<BitDegradeTreeNodeIter> node_iter;
        std::unique_ptr<LogReader::kv_iter_t> log_iter;
        int _line = 0;
        bool go_right = RIGHT_FIRST;

    public:
        BitDegradeTreeNodeIter(const BDNode * node, const IndexIter * index, const UsrJudge * judge, USR * reveal_info,
                               const uint32_t * cbegin, const uint32_t * cend)
                : _node(node), _index(index), _judge(judge), _reveal_info(reveal_info),
                  _cbegin(cbegin), _cend(cend) { next(); }

        BitDegradeTreeNodeIter(const BDNode * node, const IndexIter * index, const UsrJudge * judge, USR * reveal_info)
                : _node(node), _index(index), _judge(judge), _reveal_info(reveal_info),
                  _cbegin(node->immut_diffs().cbegin()),
                  _cend(&node->immut_diffs()[node->size() - 1]) { next(); }

        DEFAULT_MOVE(BitDegradeTreeNodeIter);
        DELETE_COPY(BitDegradeTreeNodeIter);

    public:
        ~BitDegradeTreeNodeIter() noexcept override = default;

        bool valid() const override {
            return _line != -1;
        }

        std::pair<Slice, Slice> item() const override {
            return {_item.first, _item.second};
        };

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

        void next() override {
            char mask{};
            CritPtr ptr;
            size_t nth{};

            GEN_INIT();
                if (_cend <= _cbegin + 1) {
                    // _cend == _cbegin, corner case
                    // _cend <  _cbegin, corner case
                    // _cend == _cbegin + 1, major case
                    if (_cend == _cbegin) {
                        go_right = false;
                    } else if (_cend < _cbegin) {
                        --_cbegin;
                        go_right = true;
                    }

                    do {
                        nth = _cbegin - _node->immut_diffs().cbegin();
                        mask = uint8ToChar(normalMask(_node->immut_masks()[nth]));
                        if (mask != 0) {
                            _reveal_info->reveal(*_cbegin, mask, go_right);
                        } else {
                            _reveal_info->mut_src()->resize(1);
                            _reveal_info->mut_src()->front() = 0;
                            _reveal_info->mut_extra().resize(1);
                            _reveal_info->mut_extra().front() = 0;
                        }

                        if (_judge == nullptr || _judge->possible(*_reveal_info)) {
                            ptr = *(&_node->immut_ptrs()[nth + static_cast<size_t>(go_right)]);
                            if (ptr.isNode()) {
                                node_iter = std::make_unique<BitDegradeTreeNodeIter>
                                        (_index->offToMemNode(ptr.asNode()), _index, _judge, _reveal_info);
                                while (node_iter->valid()) {
                                    // steal ownership
                                    _item = std::move(node_iter->itemRef());
                                    if (_judge == nullptr
                                        || (_reveal_info->reveal(_item.first), _judge->match(*_reveal_info))) {
                                        YIELD();
                                    }
                                    node_iter->next();
                                }
                            } else {
                                log_iter = LogReader::makeIterator(_index->_data_file, ptr.asData().val);
                                log_iter->seek(_reveal_info->toSlice());
                                _item = {log_iter->key(), log_iter->value()};
                                if (_judge == nullptr
                                    || (_reveal_info->reveal(_item.first), _judge->match(*_reveal_info))) {
                                    YIELD();
                                }
                            }
                        }

                        if (go_right == !RIGHT_FIRST || _cend != _cbegin + 1) { break; }
                        go_right = !RIGHT_FIRST;
                    } while (true);
                } else {
                    min_it = std::min_element(_cbegin, _cend, _node->functor());
                    do {
                        mask = uint8ToChar(normalMask(_node->immut_masks()[min_it - _node->immut_diffs().cbegin()]));
                        _reveal_info->reveal(*min_it, mask, go_right);

                        if (_judge == nullptr || _judge->possible(*_reveal_info)) {
                            node_iter = !go_right
                                        ? std::make_unique<BitDegradeTreeNodeIter>(
                                            _node, _index, _judge, _reveal_info, _cbegin, min_it)
                                        : std::make_unique<BitDegradeTreeNodeIter>(
                                            _node, _index, _judge, _reveal_info, min_it + 1,
                                            _cend - static_cast<int>(min_it + 1 == _cend));

                            while (node_iter->valid()) {
                                // steal ownership
                                _item = std::move(node_iter->itemRef());
                                if (_judge == nullptr
                                    || (_reveal_info->reveal(_item.first), _judge->match(*_reveal_info))) {
                                    YIELD();
                                }
                                node_iter->next();
                            }
                        }

                        if (go_right == !RIGHT_FIRST) { break; }
                        go_right = !RIGHT_FIRST;
                    } while (true);
                }
            GEN_STOP();
        }

    private:
        std::pair<Slice, std::string> & itemRef() noexcept {
            return _item;
        };
    };

    class GreaterOrEqualJudge : public UsrJudge {
    private:
        std::string _pattern;
        mutable bool _meet_first = false;

    public:
        explicit GreaterOrEqualJudge(std::string pattern) noexcept : _pattern(std::move(pattern)) {};
        DEFAULT_MOVE(GreaterOrEqualJudge);
        DELETE_COPY(GreaterOrEqualJudge);

    public:
        ~GreaterOrEqualJudge() noexcept override = default;

        bool possible(const USR & input) const override {
            return _meet_first || input.possible(_pattern);
        };

        bool match(const USR & input) const override {
            return _meet_first || (_meet_first = *input.immut_src() >= _pattern);
        };
    };

    class SmallerOrEqualJudge : public UsrJudge {
    private:
        std::string _pattern;
        mutable bool _meet_first = false;

    public:
        explicit SmallerOrEqualJudge(std::string pattern) noexcept : _pattern(std::move(pattern)) {};
        DEFAULT_MOVE(SmallerOrEqualJudge);
        DELETE_COPY(SmallerOrEqualJudge);

    public:
        ~SmallerOrEqualJudge() noexcept override = default;

        bool possible(const USR & input) const override {
            return _meet_first || input.possible(_pattern);
        };

        bool match(const USR & input) const override {
            return _meet_first || (_meet_first = *input.immut_src() <= _pattern);
        };
    };

    class IndexIter::BitDegradeTreeIterator : public Iterator<Slice, std::string> {
    private:
        const IndexIter * _index;
        std::unique_ptr<SimpleIterator<std::pair<Slice, Slice>>> _gen;
        std::unique_ptr<UsrJudge> _judge;
        USR _info;

    public:
        explicit BitDegradeTreeIterator(const IndexIter * index) noexcept
                : _index(index) { ++_index->_operating_iters; }

        DEFAULT_MOVE(BitDegradeTreeIterator);
        DELETE_COPY(BitDegradeTreeIterator);

    public:
        ~BitDegradeTreeIterator() noexcept override { --_index->_operating_iters; };

        bool valid() const override {
            return _gen != nullptr && _gen->valid();
        };

        void seekToFirst() override {
            _info.clear();
            _gen = std::make_unique<ForwardNodeIter>(_index->offToMemNode(_index->_root), _index, nullptr, &_info);
        };

        void seekToLast() override {
            _info.clear();
            _gen = std::make_unique<BackwardNodeIter>(_index->offToMemNode(_index->_root), _index, nullptr, &_info);
        };

        void seek(const Slice & target) override {
            _info.clear();
            _judge = std::make_unique<GreaterOrEqualJudge>(target.toString());
            _gen = std::make_unique<ForwardNodeIter>(_index->offToMemNode(_index->_root), _index, _judge.get(), &_info);
        };

        void next() override {
            assert(valid());

            if (!doForward()) {
                _info.clear();
                _judge = std::make_unique<GreaterOrEqualJudge>(key().toString());
                _gen = std::make_unique<ForwardNodeIter>(_index->offToMemNode(_index->_root), _index, _judge.get(),
                                                         &_info);
            }

            _gen->next();
        };

        void prev() override {
            assert(valid());

            if (!doBackward()) {
                _info.clear();
                _judge = std::make_unique<SmallerOrEqualJudge>(key().toString());
                _gen = std::make_unique<BackwardNodeIter>(_index->offToMemNode(_index->_root), _index, _judge.get(),
                                                          &_info);
            }

            _gen->next();
        };

        Slice key() const override {
            assert(valid());
            return _gen->item().first;
        };

        std::string value() const override {
            assert(valid());
            return _gen->item().second.toString();
        };

    private:
        bool doForward() const noexcept {
            return dynamic_cast<ForwardNodeIter *>(_gen.get()) != nullptr;
        };

        bool doBackward() const noexcept {
            return dynamic_cast<BackwardNodeIter *>(_gen.get()) != nullptr;
        };
    };

    class OffsetToStringIterator : public Iterator<Slice, std::string> {
    private:
        std::unique_ptr<Iterator<Slice, OffsetToData>> _offset_iter;
        RandomAccessFile * _data_file;

    public:
        OffsetToStringIterator(std::unique_ptr<Iterator<Slice, OffsetToData>> && offset_iter,
                               RandomAccessFile * data_file) noexcept
                : _offset_iter(std::move(offset_iter)), _data_file(data_file) {}

        DEFAULT_MOVE(OffsetToStringIterator);
        DELETE_COPY(OffsetToStringIterator);

    public:
        ~OffsetToStringIterator() noexcept override = default;

        bool valid() const override {
            return _offset_iter->valid();
        };

        void seekToFirst() override {
            _offset_iter->seekToFirst();
        };

        void seekToLast() override {
            _offset_iter->seekToLast();
        };

        void seek(const Slice & target) override {
            _offset_iter->seek(target);
        };

        void next() override {
            assert(valid());
            _offset_iter->next();
        };

        void prev() override {
            assert(valid());
            _offset_iter->prev();
        };

        Slice key() const override {
            assert(valid());
            return _offset_iter->key();
        };

        std::string value() const override {
            assert(valid());
            if (_offset_iter->value().val == IndexConst::disk_null_) {
                return "\1"; // del
            }
            auto kv_iter = LogReader::makeIterator(_data_file, _offset_iter->value().val);
            kv_iter->seek(key());
            return kv_iter->value();
        };
    };

    class IndexIter::TreeIteratorMerged : public MergingIterator<Slice, std::string, SliceComparator> {
    private:
        std::unique_ptr<Snapshot> _snapshot;

    public:
        TreeIteratorMerged(std::unique_ptr<BitDegradeTreeIterator> && tree_iter,
                           std::unique_ptr<OffsetToStringIterator> && pending_iter,
                           std::unique_ptr<Snapshot> && snapshot) noexcept : _snapshot(std::move(snapshot)) {
            addIterator(std::move(pending_iter));
            addIterator(std::move(tree_iter));
        }
    };

    class IndexIter::TreeIteratorFiltered : public Iterator<Slice, std::string> {
    private:
        TreeIteratorMerged _tree;

        std::string _key;
        std::string _value;
        bool _valid = false;

    public:
        TreeIteratorFiltered(std::unique_ptr<BitDegradeTreeIterator> && tree_iter,
                             std::unique_ptr<OffsetToStringIterator> && pending_iter,
                             std::unique_ptr<Snapshot> && snapshot) noexcept
                : _tree(std::move(tree_iter), std::move(pending_iter), std::move(snapshot)) {}

        DELETE_MOVE(TreeIteratorFiltered);
        DELETE_COPY(TreeIteratorFiltered);

    public:
        ~TreeIteratorFiltered() noexcept override = default;

        bool valid() const override {
            return _valid;
        };

        void seekToFirst() override {
            _key.clear();
            _tree.seekToFirst();
            update();
        };

        void seekToLast() override {
            _key.clear();
            _tree.seekToLast();
            update();
        };

        void seek(const Slice & target) override {
            _key.clear();
            _tree.seek(target);
            update();
        };

        void next() override {
            assert(valid());
            _tree.next();
            update();
        };

        void prev() override {
            assert(valid());
            _tree.prev();
            update();
        };

        Slice key() const override {
            assert(valid());
            return _key;
        };

        std::string value() const override {
            assert(valid());
            return _value;
        };

    private:
        void update() {
            bool should_break = false;
            while (_tree.valid() && !should_break) {
                std::string key = _tree.key().toString();
                std::string value = _tree.value();

                if (not(should_break = not(_key == key/* dup */|| value.back() == 1/* del */))) {
                    if (_tree.doForward()) {
                        _tree.next();
                    } else {
                        assert(_tree.doBackward());
                        _tree.prev();
                    }
                }

                _key = std::move(key);
                _value = std::move(value);
                _value.pop_back();
            }
            _valid = _tree.valid();
        }
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    IndexIter::makeIterator(std::unique_ptr<Snapshot> && snapshot) const noexcept {
        assert(snapshot != nullptr);
        return std::make_unique<TreeIteratorFiltered>(
                std::make_unique<BitDegradeTreeIterator>(this),
                std::make_unique<OffsetToStringIterator>(pendingPart(snapshot->immut_seq_num()), _data_file),
                std::move(snapshot));
    }

    void IndexIter::tryApplyPending() {
        if (_operating_iters == 0) {
            IndexRead::tryApplyPending();
        }
    }

    class IndexRegex::RegexIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        const IndexIter * _index;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;
        std::string _key;
        std::string _value;
        USR _info;
        IndexIter::ForwardNodeIter _gen;
        OffsetToStringIterator _pending_iter;
        bool _valid = false;

    public:
        RegexIterator(const IndexIter * index, std::shared_ptr<Regex::R> regex,
                      std::unique_ptr<Snapshot> && snapshot)
                : _index(index),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)),
                  _gen(_index->offToMemNode(_index->_root), _index, _regex.get(), &_info),
                  _pending_iter(_index->pendingPart(_snapshot->immut_seq_num()), _index->_data_file) {
            ++_index->_operating_iters;
            _pending_iter.seekToFirst();
            next();
        }

        DEFAULT_MOVE(RegexIterator);
        DELETE_COPY(RegexIterator);

    public:
        ~RegexIterator() noexcept override { --_index->_operating_iters; }

        bool valid() const override {
            return _valid;
        }

        std::pair<Slice, std::string> item() const override {
            return {_key, _value};
        }

        void next() override {
            bool should_break = false;
            while (!should_break) {
                Slice key_of_gen;
                if (_gen.valid()) {
                    key_of_gen = _gen.item().first;
                }
                Slice key_of_pending;
                if (_pending_iter.valid()) {
                    key_of_pending = _pending_iter.key();
                }
                if (key_of_gen.size() == 0 && key_of_pending.size() == 0) {
                    _valid = false;
                    return;
                }

                std::string key;
                std::string value;
                bool regex_check{};
                if ((regex_check = (key_of_gen.size() == 0 ||
                                    (key_of_pending.size() != 0 && !SliceComparator{}(key_of_gen, key_of_pending))))) {
                    key = key_of_pending.toString();
                    value = _pending_iter.value();
                    _pending_iter.next();
                } else {
                    key = key_of_gen.toString();
                    value = _gen.item().second.toString();
                    _gen.next();
                }

                should_break = not(_key == key || value.back() == 1 || (regex_check && !_regex->match(&key)));
                _key = std::move(key);
                _value = std::move(value);
                _value.pop_back();
            }
            _valid = true;
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    IndexRegex::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<RegexIterator>(this, std::move(regex), std::move(snapshot));
    }

    class IndexRegex::ReversedRegexIterator : public SimpleIterator<std::pair<Slice, std::string>> {
    private:
        const IndexIter * _index;
        std::shared_ptr<Regex::R> _regex;
        std::unique_ptr<Snapshot> _snapshot;
        std::string _key;
        std::string _value;
        USR _info;
        IndexIter::BackwardNodeIter _gen;
        OffsetToStringIterator _pending_iter;
        bool _valid = false;

    public:
        ReversedRegexIterator(const IndexIter * index, std::shared_ptr<Regex::R> regex,
                              std::unique_ptr<Snapshot> && snapshot)
                : _index(index),
                  _regex(std::move(regex)),
                  _snapshot(std::move(snapshot)),
                  _gen(_index->offToMemNode(_index->_root), _index, _regex.get(), &_info),
                  _pending_iter(_index->pendingPart(_snapshot->immut_seq_num()), _index->_data_file) {
            ++_index->_operating_iters;
            _pending_iter.seekToLast();
            next();
        }

        DEFAULT_MOVE(ReversedRegexIterator);
        DELETE_COPY(ReversedRegexIterator);

    public:
        ~ReversedRegexIterator() noexcept override { --_index->_operating_iters; }

        bool valid() const override {
            return _valid;
        }

        std::pair<Slice, std::string> item() const override {
            return {_key, _value};
        }

        void next() override {
            bool should_break = false;
            while (!should_break) {
                Slice key_of_gen;
                if (_gen.valid()) {
                    key_of_gen = _gen.item().first;
                }
                Slice key_of_pending;
                if (_pending_iter.valid()) {
                    key_of_pending = _pending_iter.key();
                }
                if (key_of_gen.size() == 0 && key_of_pending.size() == 0) {
                    _valid = false;
                    return;
                }

                std::string key;
                std::string value;
                bool regex_check{};
                if ((regex_check = (key_of_gen.size() == 0 || !SliceComparator{}(key_of_pending, key_of_gen)))) {
                    key = key_of_pending.toString();
                    value = _pending_iter.value();
                    _pending_iter.prev();
                } else {
                    key = key_of_gen.toString();
                    value = _gen.item().second.toString();
                    _gen.next();
                }

                should_break = not(_key == key || value.back() == 1 || (regex_check && !_regex->match(&key)));
                _key = std::move(key);
                _value = std::move(value);
                _value.pop_back();
            }
            _valid = true;
        }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    IndexRegex::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                          std::unique_ptr<Snapshot> && snapshot) const {
        return std::make_unique<ReversedRegexIterator>(this, std::move(regex), std::move(snapshot));
    }
}