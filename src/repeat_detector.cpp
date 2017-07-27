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
        STNode tmp;
        tmp.from = 1;
        tmp.to = 0;
        tmp.chunk_idx = key;
        tmp.parent = node;
        return _subs.find(tmp);
    }

    bool SuffixTree::nodeIsRoot(const STNode * node) const noexcept {
        return node->successor == node;
    }

    bool SuffixTree::nodeIsInner(const STNode * node) const noexcept {
        STNode tmp;
        tmp.from = 1;
        tmp.to = 0;
        tmp.chunk_idx = 0;
        tmp.parent = node;
        const STNode * sub = _subs.findOrGreater(tmp);
        return !nodeIsRoot(node) && sub != nullptr && sub->parent == node;
    }

    bool SuffixTree::nodeIsLeaf(const STNode * node) const noexcept {
        STNode tmp;
        tmp.from = 1;
        tmp.to = 0;
        tmp.chunk_idx = 0;
        tmp.parent = node;
        const STNode * sub = _subs.findOrGreater(tmp);
        return !nodeIsRoot(node) && (sub == nullptr || sub->parent != node);
    }

    SuffixTree::SuffixTree(Arena * arena) noexcept
            : _pool(arena),
              _root(newNode()),
              _act_node(_root),
              _edge_node(nullptr),
              _subs(arena, NodeCompare{_chunk}),
              _act_chunk_idx(0),
              _act_direct(0),
              _act_offset(0),
              _counter(0),
              _remainder(0) {
        _root->successor = _root;
    }

    std::vector<int> SuffixTree::setitem(const Slice & src) noexcept {
        assert(src.size() - 1 <= UINT16_MAX);
        auto idx = static_cast<uint16_t>(_chunk.size());
        _chunk.emplace_back(src);
        assert(_chunk.size() - 1 <= UINT16_MAX);

        _builder.send(STBuilder::STREAM_ON);
        for (int i = 0; i < src.size(); ++i) {
            insertChar(idx, char_be_uint8(src[i]));
        }
        _builder.send(STBuilder::STREAM_OFF);

        std::vector<int> res;
        std::swap(res, _builder._data);
        tryExplodeRemainder(idx);
        prepareNext();
        return res;
    }

    void SuffixTree::prepareNext() noexcept {
        assert(_remainder == 0);
        _remainder = _counter = 0;
        _act_node = _root;
        _act_chunk_idx = _act_direct = _act_offset = 0;
    }

    void SuffixTree::insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept {
        ++_remainder;
        const Slice & curr_s = _chunk[chunk_idx];

        auto case_root = [&](bool send_msg) noexcept {
            _edge_node = nodeGetSub(_root, msg_char);
            if (_edge_node == nullptr) {
                STNode leaf_node;
                leaf_node.successor = _root;
                leaf_node.chunk_idx = chunk_idx;
                leaf_node.from = _counter;
                leaf_node.to = static_cast<uint16_t>(curr_s.size());
                leaf_node.parent = _root;

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
            assert(_edge_node == nodeGetSub(_act_node, char_be_uint8(edge_s[_act_direct])));

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
                       && msg_char == char_be_uint8(edge_s[_edge_node->from + _act_offset])) {
                _builder.send(_edge_node->chunk_idx, _edge_node->from + _act_offset, msg_char);
                ++_act_offset;
            } else {
                _builder.send(STBuilder::STREAM_PASS, 0/* placeholder */, msg_char);
                const STNode * prev_inner_node = nullptr;

                auto split_grow = [&]() noexcept {
                    STNode leaf_node;
                    leaf_node.successor = _root;
                    leaf_node.chunk_idx = chunk_idx;
                    leaf_node.from = _counter;
                    leaf_node.to = static_cast<uint16_t>(curr_s.size());
                    --_remainder;

                    if ((nodeIsLeaf(_edge_node) || _edge_node->to - _edge_node->from > 1)
                        && _edge_node->from + _act_offset != _edge_node->to) {
                        STNode inner_node;
                        inner_node.successor = _root;
                        inner_node.chunk_idx = _edge_node->chunk_idx;
                        inner_node.from = _edge_node->from;
                        inner_node.to = static_cast<uint16_t>(_edge_node->from + _act_offset);

                        inner_node.parent = nullptr;
                        const STNode * inner_node_ = nodeSetSub(inner_node);
                        inner_node.parent = _edge_node->parent;

                        if (prev_inner_node != nullptr) {
                            const_cast<STNode *>(prev_inner_node)->successor = inner_node_;
                        }
                        prev_inner_node = inner_node_;

                        STNode tmp;
                        tmp = *_edge_node;
                        tmp.from = inner_node.to;
                        tmp.parent = inner_node_;
                        _subs.move_to_fit(*_edge_node, tmp);

                        tmp = inner_node;
                        tmp.parent = nullptr;
                        _subs.move_to_fit(tmp, inner_node);

                        leaf_node.parent = inner_node_;
                        nodeSetSub(leaf_node);
                    } else {
                        if (prev_inner_node != nullptr) {
                            const_cast<STNode *>(prev_inner_node)->successor = _edge_node;
                        }
                        prev_inner_node = _edge_node;

                        leaf_node.parent = _edge_node;
                        nodeSetSub(leaf_node);
                    }
                };

                auto overflow_fix = [&]() noexcept {
                    uint16_t end = _counter;
                    uint16_t begin = end - _act_offset;
                    _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s[_counter - _act_offset]));
                    edge_s = _chunk[_edge_node->chunk_idx];

                    int supply;
                    while (end - begin > (supply = _edge_node->to - _edge_node->from)) {
                        _act_node = _edge_node;
                        begin += supply;
                        _act_offset -= supply;

                        _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s[begin]));
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
                    }
                    if (_edge_node->from + _act_offset < _edge_node->to
                        && msg_char == char_be_uint8(edge_s[_edge_node->from + _act_offset])) {
                        ++_act_offset;
                        break;
                    }
                }

                if (_remainder == 1) {
                    _builder.send(STBuilder::STREAM_POP);
                    _builder.send(_act_chunk_idx, _act_direct, msg_char);
                }
            }
        }
        ++_counter;
    }

    void SuffixTree::tryExplodeRemainder(uint16_t chunk_idx) noexcept {
        const Slice & curr_s = _chunk[chunk_idx];
        const STNode * prev_inner_node = nullptr;

        auto split = [&]() noexcept {
            --_remainder;
            if ((nodeIsLeaf(_edge_node) || _edge_node->to - _edge_node->from > 1)
                && _edge_node->from + _act_offset != _edge_node->to) {
                STNode inner_node;
                inner_node.successor = _root;
                inner_node.chunk_idx = _edge_node->chunk_idx;
                inner_node.from = _edge_node->from;
                inner_node.to = static_cast<uint16_t>(_edge_node->from + _act_offset);

                inner_node.parent = nullptr;
                const STNode * inner_node_ = nodeSetSub(inner_node);
                inner_node.parent = _edge_node->parent;

                if (prev_inner_node != nullptr) {
                    const_cast<STNode *>(prev_inner_node)->successor = inner_node_;
                }
                prev_inner_node = inner_node_;

                STNode tmp;
                tmp = *_edge_node;
                tmp.from = inner_node.to;
                tmp.parent = inner_node_;
                _subs.move_to_fit(*_edge_node, tmp);

                tmp = inner_node;
                tmp.parent = nullptr;
                _subs.move_to_fit(tmp, inner_node);
            } else {
                if (prev_inner_node != nullptr) {
                    const_cast<STNode *>(prev_inner_node)->successor = _edge_node;
                }
                prev_inner_node = _edge_node;
            }
        };

        auto overflow_fix = [&]() noexcept {
            uint16_t end = _counter;
            uint16_t begin = end - _act_offset;
            _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s[_counter - _act_offset]));

            int supply;
            while (end - begin > (supply = _edge_node->to - _edge_node->from)) {
                _act_node = _edge_node;
                begin += supply;
                _act_offset -= supply;

                _edge_node = nodeGetSub(_act_node, char_be_uint8(curr_s[begin]));
                _act_direct = _edge_node->from;
            }
        };

        while (_remainder > 0) {
            split();
            if (!nodeIsInner(_act_node)) {
                --_act_offset;
                ++_act_direct;

                if (_act_offset > 0) {
                    overflow_fix();
                }
            } else {
                _act_node = _act_node->successor;
                overflow_fix();
            }
        }
    }

    void STBuilder::send(int chunk_idx_or_cmd, int s_idx, int msg_char) noexcept {
        auto try_explode = [&]() noexcept {
            static constexpr int compress_cost = 1/* FN */+ 1/* chunk_idx */+ 1/* from */+ 1/* to */;
            if (_compress_len > compress_cost) {
                _data.resize(_data.size() - _compress_len);
                _data.insert(_data.end(), {CoderConst::FN,
                                           _compress_idx,
                                           _compress_to - _compress_len,
                                           _compress_to});
            }
            _compress_len = 0;
        };

        auto set_record = [&]() noexcept {
            _compress_idx = chunk_idx_or_cmd;
            _compress_to = s_idx + 1;
            ++_compress_len;
        };

        switch (chunk_idx_or_cmd) {
            case STREAM_ON:
                _compress_len = 0;
                break;
            case STREAM_PASS:
                try_explode();
                _data.emplace_back(msg_char);
                break;
            case STREAM_OFF:
                try_explode();
                assert(_data.size() <= UINT16_MAX);
                break;
            case STREAM_POP:
                _data.pop_back();
                break;
            default:
                set_record();
                _data.emplace_back(msg_char);
                break;
        }
    }

    std::string SuffixTree::toString() const noexcept {
        std::string res;

        auto print_node = [&](const STNode * node) noexcept {
            if (node == _act_node) {
                res += '*';
            }
            res.append(_chunk[node->chunk_idx].data() + node->from, node->to - node->from);
        };

        std::function<void(const STNode *, size_t)> print_tree = [&](const STNode * node, size_t lv) noexcept {
            if (lv > 0) {
                res += std::string((lv - 1) * 2, ' ') + "--";
                print_node(node);
            } else {
                res += '#';
            }
            res += '\n';

            ++lv;
            SkipList<STNode, NodeCompare>::Iterator it(&_subs);
            STNode tmp;
            tmp.from = 1;
            tmp.to = 0;
            tmp.chunk_idx = 0;
            tmp.parent = node;
            for (it.seek(tmp);
                 it.valid() && it.key().parent == node;
                 it.next()) {
                print_tree(&it.key(), lv);
            }
        };

        print_tree(_root, 0);
        return res;
    }
}