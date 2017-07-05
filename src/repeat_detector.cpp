#include "repeat_detector.h"

namespace LeviDB {
    STNode * SuffixTree::newNode() noexcept {
        return reinterpret_cast<STNode *>(_pool->allocateAligned(sizeof(STNode)));
    }

    STNode * SuffixTree::nodeGetSub(STNode * node, uint8_t key) const noexcept {
        return this->_subs.find(STNode{.from=1, .to=0, .parent=node, .chunk_idx=key});
    }

    STNode * SuffixTree::nodeSetSub(const STNode & sub) noexcept {
        return this->_subs.insert(sub);
    }

    bool SuffixTree::nodeIsRoot(STNode * node) const noexcept {
        return node->successor == node;
    }

    bool SuffixTree::nodeIsInner(STNode * node) const noexcept {
        return !nodeIsRoot(node) &&
               this->_subs.findOrGreater(STNode{.from=1, .to=0, .chunk_idx=0, .parent=node})->parent == node;
    }

    bool SuffixTree::nodeIsLeaf(STNode * node) const noexcept {
        return !nodeIsRoot(node) &&
               this->_subs.findOrGreater(STNode{.from=1, .to=0, .chunk_idx=0, .parent=node})->parent != node;
    }

    SuffixTree::SuffixTree(Arena * arena) noexcept
            : _root(newNode()),
              _act_node(_root),
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

    static_assert(sizeof(uint8_t) == sizeof(char), "uint8_t != char");

    std::vector<int> SuffixTree::setitem(const Slice & src) noexcept {
        uint16_t idx = static_cast<uint16_t>(_chunk.size());
        _chunk.emplace_back(src);

        _builder.send(STBuilder::STREAM_ON);
        for (size_t i = 0; i < src.size(); ++i) {
            insertChar(idx, static_cast<uint8_t>(src.data()[i]));
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
            STNode * edge_node = nodeGetSub(_root, msg_char);
            if (edge_node == nullptr) {
                STNode leaf_node = {
                        .successor=_root,
                        .chunk_idx=chunk_idx,
                        .from=_counter,
                        .to=static_cast<uint16_t>(curr_s.size()),
                        .parent=_root,
                };
                nodeSetSub(leaf_node);
                --_remainder;
                if (send_msg) {
                    _builder.send(STBuilder::STREAM_PASS, INT_MIN/* placeholder */, msg_char);
                }
            } else {
                _act_chunk_idx = edge_node->chunk_idx;
                _act_direct = edge_node->from;
                ++_act_offset;
                assert(_act_offset == 1);
                if (send_msg) {
                    _builder.send(edge_node->chunk_idx, edge_node->from, msg_char);
                }
            }
        };

        if (nodeIsRoot(_act_node) && _act_offset == 0) {
            case_root(true);
        } else {
            Slice edge_s = _chunk[_act_chunk_idx];
            STNode * edge_node = nodeGetSub(_act_node,
                                            static_cast<uint8_t>(edge_s.data()[_act_direct]));

            STNode * next_edge_node;
            if (edge_node->from + _act_offset == edge_node->to
                && (next_edge_node = nodeGetSub(edge_node, msg_char))) {
                _act_node = edge_node;
                _act_chunk_idx = next_edge_node->chunk_idx;
                _act_direct = next_edge_node->from;
                _builder.send(next_edge_node->chunk_idx, next_edge_node->from, msg_char);
            } else if (edge_node->from + _act_offset < edge_node->to
                       && msg_char == edge_s.data()[edge_node->from + _act_offset]) {
                _builder.send(edge_node->chunk_idx, edge_node->from + _act_offset, msg_char);
                ++_act_offset;
            } else {
                _builder.send(STBuilder::STREAM_PASS, msg_char);
                STNode * prev_inner_node = nullptr;

                auto split_grow = [&]() {
                    STNode leaf_node = {
                            .successor=_root,
                            .chunk_idx=chunk_idx,
                            .from=_counter,
                            .to=static_cast<uint16_t>(curr_s.size()),
                    };
                    --_remainder;

                    if ((nodeIsLeaf(edge_node) || edge_node->to - edge_node->from > 1)
                        && edge_node->from + _act_offset != edge_node->to) {
                        STNode inner_node = {
                                .successor=_root,
                                .chunk_idx=edge_node->chunk_idx,
                                .from=edge_node->from,
                                .to=static_cast<uint16_t>(edge_node->from + _act_offset),
                                .parent=edge_node->parent,
                        };

                        STNode * inner_node_ptr = nodeSetSub(inner_node);;
                        if (prev_inner_node != nullptr) {
                            prev_inner_node->successor = inner_node_ptr;
                        }
                        prev_inner_node = inner_node_ptr;

                        edge_node->from = inner_node.to;
                        edge_node->parent = inner_node_ptr;

                        leaf_node.parent = inner_node_ptr;
                        nodeSetSub(*edge_node);
                        nodeSetSub(leaf_node);
                    }
                };

                auto overflow_fix = [&]() {
                    uint16_t end = _counter;
                    uint16_t begin = end - _act_offset;

                    edge_node = nodeGetSub(_act_node,
                                           static_cast<uint8_t>(curr_s.data()[_counter - _act_offset]));
                    edge_s = _chunk[edge_node->chunk_idx];

                    int supply;
                    while (end - begin > (supply = edge_node->to - edge_node->from)) {
                        _act_node = edge_node;
                        begin += supply;
                        _act_offset -= supply;

                        edge_node = nodeGetSub(_act_node, static_cast<uint8_t>(curr_s.data()[begin]));
                        edge_s = _chunk[edge_node->chunk_idx];
                        _act_direct = edge_node->from;
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
                        }
                    } else {
                        _act_node = _act_node->successor;
                        overflow_fix();
                    }

                    if (edge_node->from + _act_offset == edge_node->to
                        && (next_edge_node = nodeGetSub(edge_node, msg_char))) {
                        _act_node = edge_node;
                        _act_chunk_idx = next_edge_node->chunk_idx;
                        _act_direct = next_edge_node->from;
                        _act_offset = 1;

                        if (prev_inner_node != nullptr) {
                            prev_inner_node->successor = _act_node;
                        }
                        break;
                    } else if (edge_node->from + _act_offset < edge_node->to
                               && msg_char == edge_s.data()[edge_node->from + _act_offset]) {
                        ++_act_offset;
                        break;
                    }
                }
            }
        }
        ++_counter;
    }
}