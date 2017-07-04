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

    std::vector<int> SuffixTree::setitem(const Slice & src) noexcept {
        uint16_t idx = static_cast<uint16_t >(_chunk.size());
        _chunk.emplace_back(src);

        _builder.send(STBuilder::STREAM_ON);
        for (size_t i = 0; i < src.size(); ++i) {
            insertChar(idx, static_cast<uint8_t>(src.data()[i]));
        }
        _builder.send(STBuilder::STREAM_OFF);

        std::vector<int> res;
        std::swap(res, _builder.res);
        prepareNext();
        return res;
    }

    void SuffixTree::prepareNext() noexcept {
        _remainder = _counter = 0;
        _act_node = _root;
        _act_chunk_idx = _act_direct = _act_offset = 0;
    }

    void SuffixTree::insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept {

    }
}