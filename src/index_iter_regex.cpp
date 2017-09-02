#ifndef __clang__
#include <algorithm>
#endif

#include "index_iter_regex.h"
#include "log_reader.h"

namespace LeviDB {
    class IndexIter::BitDegradeTreeNodeIter {
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
        std::unique_ptr<LogReader::kv_iter> log_iter;
        int _line = 0;
        bool go_right = false;

    public:
        BitDegradeTreeNodeIter(const BDNode * node, const IndexIter * index, const UsrJudge * judge, USR * reveal_info,
                               const uint32_t * cbegin, const uint32_t * cend)
                : _node(node), _index(index), _judge(judge), _reveal_info(reveal_info),
                  _cbegin(cbegin), _cend(cend) { next(); }

        BitDegradeTreeNodeIter(const BDNode * node, const IndexIter * index, const UsrJudge * judge, USR * reveal_info)
                : _node(node), _index(index), _judge(judge), _reveal_info(reveal_info),
                  _cbegin(node->immut_diffs().cbegin()),
                  _cend(&node->immut_diffs()[node->size() - 1]) { next(); }

        bool valid() const noexcept {
            return _line != -1;
        }

        const std::pair<Slice, std::string> & item() const noexcept {
            return _item;
        };

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

        void next() {
            CritPtr ptr;
            size_t nth{};
            char mask = 0;

            GEN_INIT();
                if (_cend <= _cbegin + 1) {
                    // _cend == _cbegin, corner case
                    // _cend == _cbegin + 1, major case
                    do {
                        nth = _cbegin - _node->immut_diffs().cbegin();
                        if (go_right) {
                            mask = uint8ToChar(_node->immut_masks()[nth]);
                        }
                        _reveal_info->reveal(*_cbegin, mask);

                        if (_judge == nullptr || _judge->possible(*_reveal_info)) {
                            ptr = *(&_node->immut_ptrs()[nth + static_cast<size_t>(go_right)]);
                            if (ptr.isNode()) {
                                node_iter = std::make_unique<BitDegradeTreeNodeIter>
                                        (_index->offToMemNode(ptr.asNode()), _index, _judge, _reveal_info);
                                while (node_iter->valid()) {
                                    // steal ownership
                                    _item = std::move(const_cast<std::pair<Slice, std::string> &>(node_iter->item()));
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

                        if (go_right || _cend != _cbegin + 1) { break; }
                        go_right = true;
                    } while (true);
                } else {
                    min_it = std::min_element(_cbegin, _cend, _node->functor());
                    do {
                        if (go_right) {
                            mask = uint8ToChar(_node->immut_masks()[min_it - _node->immut_diffs().cbegin()]);
                        }
                        _reveal_info->reveal(*min_it, mask);

                        if (_judge == nullptr || _judge->possible(*_reveal_info)) {
                            node_iter = !go_right
                                        ? std::make_unique<BitDegradeTreeNodeIter>(
                                            _node, _index, _judge, _reveal_info, _cbegin, min_it)
                                        : std::make_unique<BitDegradeTreeNodeIter>(
                                            _node, _index, _judge, _reveal_info, min_it + 1, _cend);

                            while (node_iter->valid()) {
                                // steal ownership
                                _item = std::move(const_cast<std::pair<Slice, std::string> &>(node_iter->item()));
                                if (_judge == nullptr
                                    || (_reveal_info->reveal(_item.first), _judge->match(*_reveal_info))) {
                                    YIELD();
                                }
                                node_iter->next();
                            }
                        }

                        if (go_right) { break; }
                        go_right = true;
                    } while (true);
                }
            GEN_STOP();
        }
    };

    class IndexIter::BitDegradeTreeIterator : public Iterator<Slice, std::string> {
    private:
        const IndexIter * _index;
        std::unique_ptr<BitDegradeTreeNodeIter> _gen;
        std::unique_ptr<Snapshot> _snapshot;
        std::unique_ptr<Iterator<Slice, OffsetToData>> _pending;
        std::unique_ptr<UsrJudge> _judge;
        USR _info;

    public:
        explicit BitDegradeTreeIterator(const IndexIter * index, std::unique_ptr<Snapshot> && snapshot) noexcept
                : _index(index), _snapshot(std::move(snapshot)),
                  _pending(_index->pendingPart(_snapshot->immut_seq_num())) { ++_index->operating_iters; }

        DEFAULT_MOVE(BitDegradeTreeIterator);
        DELETE_COPY(BitDegradeTreeIterator);

    public:
        ~BitDegradeTreeIterator() noexcept override { --_index->operating_iters; };

        EXPOSE(_pending);

        bool valid() const override {
            return _gen != nullptr && _gen->valid();
        };

        void seekToFirst() override {
            _gen = std::make_unique<BitDegradeTreeNodeIter>(_index->offToMemNode(_index->_root), _index, _judge.get(),
                                                            &_info);
        };

        void seekToLast() override {

        };

        void seek(const Slice & target) override {

        };

        void next() override {
            assert(valid());
            _gen->next();
        };

        void prev() override {
            assert(valid());
        };

        Slice key() const override {
            assert(valid());
            return _gen->item().first;
        };

        std::string value() const override {
            assert(valid());
            return _gen->item().second;
        };
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    IndexIter::makeIterator(std::unique_ptr<Snapshot> && snapshot) const noexcept {
        return std::make_unique<BitDegradeTreeIterator>(this, snapshot == nullptr
                                                              ? _seq_gen->makeSnapshot()
                                                              : std::move(snapshot));
    }

    void IndexIter::tryApplyPending() {
        if (operating_iters == 0) {
            IndexRead::tryApplyPending();
        }
    }
}