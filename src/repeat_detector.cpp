#include "repeat_detector.h"

namespace LeviDB {
    STNode * SuffixTree::newNode() noexcept {
        return reinterpret_cast<STNode *>(_pool->allocateAligned(sizeof(STNode)));
    }

    STNode * SuffixTree::nodeGetSub(STNode * node, uint8_t key) const noexcept {
        return this->_subs.find(STNode{.from=1, .to=0, .parent=node, .chunk_idx=key});
    }

    void SuffixTree::nodeSetSub(const STNode & sub) noexcept {
        this->_subs.insert(sub);
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

    static_assert(sizeof(uint8_t) == sizeof(char));

    std::vector<int> SuffixTree::setitem(const Slice & src) noexcept {
        uint16_t idx = static_cast<uint16_t>(_chunk.size());
        _chunk.emplace_back(src);

        _builder.send(STBuilder::STREAM_ON);
        for (size_t i = 0; i < src.size(); ++i) {
            insertChar(idx, reinterpret_cast<uint8_t>(src.data()[i]));
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
            const Slice & edge_s = _chunk[_act_chunk_idx];
            STNode * edge_node = nodeGetSub(_act_node, edge_s.data()[_act_direct]);
        }
    }
}