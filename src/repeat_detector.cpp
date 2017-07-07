#include "coder.h"
#include "repeat_detector.h"

namespace LeviDB {
    STNode * SuffixTree::newNode() noexcept {
        return reinterpret_cast<STNode *>(_pool->allocateAligned(sizeof(STNode)));
    }

    const STNode * SuffixTree::nodeSetSub(const STNode & sub) noexcept {
        return _subs.insert(sub);
    }

    const STNode * SuffixTree::nodeGetSub(const STNode * node, uint8_t key) const noexcept {
        return _subs.find(STNode{.from=1, .to=0, .parent=node, .chunk_idx=key});
    }

    bool SuffixTree::nodeIsRoot(const STNode * node) const noexcept {
        return node->successor == node;
    }

    bool SuffixTree::nodeIsInner(const STNode * node) const noexcept {
        const STNode * sub = _subs.findOrGreater(STNode{.from=1, .to=0, .chunk_idx=0, .parent=node});
        return !nodeIsRoot(node) && sub != nullptr && sub->parent == node;
    }

    bool SuffixTree::nodeIsLeaf(const STNode * node) const noexcept {
        const STNode * sub = _subs.findOrGreater(STNode{.from=1, .to=0, .chunk_idx=0, .parent=node});
        return !nodeIsRoot(node) && (sub == nullptr || sub->parent != node);
    }

    SuffixTree::SuffixTree(Arena * arena) noexcept
            : _root(newNode()),
              _act_node(_root),
              _edge_node(nullptr),
              _pool(arena),
              _chunk(),
              _subs(arena, NodeCompare{_chunk}),
              _builder(),
              _act_chunk_idx(0),
              _act_direct(0),
              _act_offset(0),
              _counter(0),
              _remainder(0) {
        _root->successor = _root;
    }

    std::vector<int> SuffixTree::setitem(const Slice & src) noexcept {
        uint16_t idx = static_cast<uint16_t>(_chunk.size());
        _chunk.emplace_back(src);

        _builder.send(STBuilder::STREAM_ON);
        for (size_t i = 0; i < src.size(); ++i) {
            insertChar(idx, char_be_uint8(src.data()[i]));
        }
        _builder.send(STBuilder::STREAM_OFF);

        std::vector<int> res;
        std::swap(res, _builder._data);
        prepareNext();
        return res;
    }

    void SuffixTree::prepareNext() noexcept {
        _remainder = _counter = 0;
        _act_node = _root;
        _act_chunk_idx = _act_direct = _act_offset = 0;
    }

    void SuffixTree::insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept {
        ++_remainder;
        const Slice & curr_s = _chunk[chunk_idx];

        auto case_root = [&](bool send_msg) {
            _edge_node = nodeGetSub(_root, msg_char);
            if (_edge_node == nullptr) {
                STNode leaf_node = {
                        .successor=_root,
                        .chunk_idx=chunk_idx,
                        .from=_counter,
                        .to=static_cast<uint16_t>(curr_s.size()),
                        .parent=_root,
                };
                _edge_node = nodeSetSub(leaf_node);
                --_remainder;
                if (send_msg) {
                    _builder.send(STBuilder::STREAM_PASS, 0/* placeholder */, msg_char);
                }
            } else {
                _act_chunk_idx = _edge_node->chunk_idx;
                _act_direct = _edge_node->from;
                ++_act_offset;
                assert(_act_offset == 1);
                if (send_msg) {
                    _builder.send(_edge_node->chunk_idx, _edge_node->from, msg_char);
                }
            }
        };

        if (nodeIsRoot(_act_node) && _act_offset == 0) {
            case_root(true);
        } else {
            Slice edge_s = _chunk[_act_chunk_idx];
            assert(_edge_node == nodeGetSub(_act_node, char_be_uint8(edge_s.data()[_act_direct])));

            const STNode * next_edge_node;
            if (_edge_node->from + _act_offset == _edge_node->to
                && (next_edge_node = nodeGetSub(_edge_node, msg_char))) {
                _act_node = _edge_node;
                _act_chunk_idx = next_edge_node->chunk_idx;
                _act_direct = next_edge_node->from;
                _act_offset = 1;
                _edge_node = next_edge_node;
                _builder.send(next_edge_node->chunk_idx, next_edge_node->from, msg_char);
            } else if (_edge_node->from + _act_offset < _edge_node->to
                       && msg_char == edge_s.data()[_edge_node->from + _act_offset]) {
                _builder.send(_edge_node->chunk_idx, _edge_node->from + _act_offset, msg_char);
                ++_act_offset;
            } else {
                _builder.send(STBuilder::STREAM_PASS, 0/* placeholder */, msg_char);
                const STNode * prev_inner_node = nullptr;

                auto split_grow = [&]() {
                    STNode leaf_node = {
                            .successor=_root,
                            .chunk_idx=chunk_idx,
                            .from=_counter,
                            .to=static_cast<uint16_t>(curr_s.size()),
                    };
                    --_remainder;

                    if ((nodeIsLeaf(_edge_node) || _edge_node->to - _edge_node->from > 1)
                        && _edge_node->from + _act_offset != _edge_node->to) {
                        STNode inner_node = {
                                .successor=_root,
                                .chunk_idx=_edge_node->chunk_idx,
                                .from=_edge_node->from,
                                .to=static_cast<uint16_t>(_edge_node->from + _act_offset),
                                .parent=_edge_node->parent,
                        };

                        SkipList<STNode, NodeCompare>::Iterator it(&_subs);
                        std::vector<STNode> edge_subs;
                        for (it.seek(STNode{.from=1, .to=0, .chunk_idx=0, .parent=_edge_node});
                             it.valid() && it.key().parent == _edge_node;
                             it.next()) {
                            edge_subs.push_back(it.key());
                        }
                        for (int i = 0; i < edge_subs.size(); ++i) {
                            STNode tmp = edge_subs[i];
                            tmp.parent = nullptr;
                            _subs.move_to_fit(edge_subs[i], tmp);
                            edge_subs[i] = tmp;
                        }

                        STNode edge_node_ = *_edge_node;
                        edge_node_.from = inner_node.to;

                        const STNode * inner_node_ = nodeSetSub(inner_node);
                        if (prev_inner_node != nullptr) {
                            const_cast<STNode *>(prev_inner_node)->successor = inner_node_;
                        }
                        prev_inner_node = inner_node_;
                        leaf_node.parent = inner_node_;
                        edge_node_.parent = inner_node_;

                        nodeSetSub(leaf_node);
                        _edge_node = nodeSetSub(edge_node_);
                        for (const STNode & node:edge_subs) {
                            STNode tmp = node;
                            tmp.parent = _edge_node;
                            _subs.move_to_fit(node, tmp);
                        }
                    } else {
                        if (prev_inner_node != nullptr) {
                            const_cast<STNode *>(prev_inner_node)->successor = _edge_node;
                        }
                        prev_inner_node = _edge_node;

                        leaf_node.parent = _edge_node;
                        nodeSetSub(leaf_node);
                    }
                };

                auto overflow_fix = [&]() {
                    uint16_t end = _counter;
                    uint16_t begin = end - _act_offset;
                    assert(_edge_node == nodeGetSub(_act_node, char_be_uint8(curr_s.data()[_counter - _act_offset])));
                    edge_s = _chunk[_edge_node->chunk_idx];

                    int supply;
                    while (end - begin > (supply = _edge_node->to - _edge_node->from)) {
                        _act_node = _edge_node;
                        begin += supply;
                        _act_offset -= supply;

                        _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s.data()[begin]));
                        edge_s = _chunk[_edge_node->chunk_idx];
                        _act_direct = _edge_node->from;
                    }
                };

                while (_remainder > 0) {
                    split_grow();
                    if (!nodeIsInner(_act_node)) {
                        --_act_offset;
                        ++_act_direct;

                        if (_act_offset > 0) {
                            overflow_fix();
                        } else {
                            case_root(false);
                            break;
                        }
                    } else {
                        _act_node = _act_node->successor;
                        _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s.data()[_counter - _act_offset]));
                        overflow_fix();
                    }

                    if (_edge_node->from + _act_offset == _edge_node->to
                        && (next_edge_node = nodeGetSub(_edge_node, msg_char))) {
                        _act_node = _edge_node;
                        _act_chunk_idx = next_edge_node->chunk_idx;
                        _act_direct = next_edge_node->from;
                        _act_offset = 1;
                        _edge_node = next_edge_node;

                        if (prev_inner_node != nullptr) {
                            const_cast<STNode *>(prev_inner_node)->successor = _act_node;
                        }
                        break;
                    } else if (_edge_node->from + _act_offset < _edge_node->to
                               && msg_char == edge_s.data()[_edge_node->from + _act_offset]) {
                        ++_act_offset;
                        break;
                    }
                }
            }
        }
        ++_counter;
    }

    void STBuilder::send(int chunk_idx_or_cmd, int s_idx, int msg_char) noexcept {
        auto try_explode = [&]() {
            static constexpr int compress_cost = 1/* FN */ + 2/* chunk_idx */ + 2/* from */ + 2/* to */;
            if (_compress_len > compress_cost) {
                _data.resize(_data.size() - _compress_len);
                _data.insert(_data.end(), {CoderConst::FN,
                                           _compress_idx,
                                           _compress_to - _compress_len,
                                           _compress_to});
            }
            _compress_len = 0;
        };

        auto set_record = [&]() {
            _compress_idx = chunk_idx_or_cmd;
            _compress_to = s_idx + 1;
            ++_compress_len;
        };

        switch (chunk_idx_or_cmd) {
            case STBuilder::STREAM_ON:
                _compress_len = 0;
                break;
            case STBuilder::STREAM_PASS:
                try_explode();
                _data.emplace_back(msg_char);
                break;
            case STBuilder::STREAM_OFF:
                try_explode();
                assert(_data.size() <= UINT16_MAX);
                break;
            default:
                set_record();
                _data.emplace_back(msg_char);
                break;
        }
    }

    std::string SuffixTree::toString() const noexcept {
        std::string res;

        auto print_node = [&](const STNode * node) {
            if (node == _act_node) {
                res += '*';
            }
            res.append(_chunk[node->chunk_idx].data() + node->from, node->to - node->from);
        };

        std::function<void(const STNode *, int)> print_tree = [&](const STNode * node, int lv) {
            if (lv > 0) {
                res += std::string(static_cast<size_t>(lv - 1) * 2, ' ') + "--";
                print_node(node);
            } else {
                res += '#';
            }
            res += '\n';

            ++lv;
            SkipList<STNode, NodeCompare>::Iterator it(&_subs);
            for (it.seek(STNode{.from=1, .to=0, .chunk_idx=0, .parent=node});
                 it.valid() && it.key().parent == node;
                 it.next()) {
                print_tree(&it.key(), lv);
            }
        };

        print_tree(_root, 0);
        return res;
    }
}