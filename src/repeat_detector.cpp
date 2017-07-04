#include "repeat_detector.h"

namespace LeviDB {
    STNode * SuffixTree::newNode() noexcept {
        return reinterpret_cast<STNode *>(_pool->allocateAligned(sizeof(STNode)));
    }

    void SuffixTree::nodeSetSub(STNode sub) noexcept {
        this->_subs.insert(sub);
    }

    STNode * SuffixTree::nodeGetSub(STNode * node, uint8_t key) const noexcept {
        return this->_subs.find(STNode{.from=1, .to=0, .parent=node, .chunk_idx=key});
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
}