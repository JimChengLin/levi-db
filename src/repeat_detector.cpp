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
              _subs(arena),
              _builder(),
              _chunk(),
              _act_chunk_idx(0),
              _act_direct(0),
              _act_offset(0),
              _counter(0),
              _remainder(0) {
        _root->successor = _root;
    }

    void SuffixTree::setitem(const Slice & src) noexcept {

    }

    void SuffixTree::prepareNext() noexcept {
        _remainder = _counter = 0;
        _act_node = _root;
        _act_chunk_idx = _act_direct = _act_offset = 0;
    }
}