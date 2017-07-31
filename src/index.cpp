#ifndef __clang__
#include <algorithm>
#endif

#include "index.h"

namespace LeviDB {
    OffsetToData BitDegradeTree::find(const Slice & k) const {
        const BDNode * cursor = offToMemNode(_root);

        while (true) {
            auto pos = findBestMatch(cursor, k);
            size_t idx;
            bool direct;
            std::tie(idx, direct, std::ignore) = pos;

            CritPtr ptr = cursor->immut_ptrs()[idx + static_cast<size_t>(direct)];
            if (ptr.isNull()) {
                return {IndexConst::disk_null_};
            }
            if (ptr.isNode()) {
                cursor = offToMemNode(ptr.asNode());
            } else {
                return ptr.asData();
            }
        }
    }

    size_t BitDegradeTree::size(const BDNode * node) const {
        size_t cnt = 0;
        for (CritPtr ptr:node->immut_ptrs()) {
            if (ptr.isNull()) {
                break;
            }
            if (ptr.isData()) {
                ++cnt;
            } else {
                cnt += size(offToMemNode(ptr.asNode()));
            }
        }
        return cnt;
    }

    void BitDegradeTree::insert(const Slice & k, OffsetToData v) {
        BDNode * cursor = offToMemNode(_root);

        while (true) {
            auto pos = findBestMatch(cursor, k);
            size_t idx;
            bool direct;
            std::tie(idx, direct, std::ignore) = pos;

            CritPtr & ptr = cursor->mut_ptrs()[idx + static_cast<size_t>(direct)];
            if (ptr.isNull()) {
                ptr.setData(v);
                cursor->updateChecksum();
                break;
            }
            if (ptr.isNode()) {
                cursor = offToMemNode(ptr.asNode());
            } else {
                std::unique_ptr<Matcher> matcher = offToMatcher(ptr.asData());
                if (*matcher == k) {
                } else {
                    combatInsert(matcher->toString(), k, v);
                }
                break;
            }
        }
    }

    void BitDegradeTree::remove(const Slice & k) {
        BDNode * parent = nullptr;
        std::tuple<size_t, bool, size_t> parent_info;

        BDNode * cursor = offToMemNode(_root);
        while (true) {
            auto pos = findBestMatch(cursor, k);
            size_t idx;
            bool direct;
            size_t size;
            std::tie(idx, direct, size) = pos;

            CritPtr ptr = cursor->immut_ptrs()[idx + static_cast<size_t>(direct)];
            if (ptr.isNull()) {
                break;
            }
            if (ptr.isNode()) {
                parent = cursor;
                parent_info = pos;
                cursor = offToMemNode(ptr.asNode());
            } else {
                std::unique_ptr<Matcher> matcher = offToMatcher(ptr.asData());
                if (*matcher == k) {
                    nodeRemove(cursor, idx, direct, size);
                    if (parent != nullptr) {
                        tryMerge(parent, cursor,
                                 std::get<0>(parent_info), std::get<1>(parent_info), std::get<2>(parent_info),
                                 size - 1);
                    }
                }
                break;
            }
        }
    }

    std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
    BitDegradeTree::findBestMatch(const BDNode * node, const Slice & k) const noexcept {
        const uint32_t * cbegin = node->immut_diffs().cbegin();
        const uint32_t * cend;

        size_t size = node->size();
        if (size <= 1) {
            return {0, false, size};
        }
        cend = &node->immut_diffs()[size - 1];

        auto cmp = node->functor();
        std::unique_ptr<Matcher> matcher = sliceToMatcher(k);
        while (true) {
            const uint32_t * min_it = std::min_element(cbegin, cend, cmp);
            uint32_t diff_at = *min_it;

            // left or right?
            uint8_t crit_byte = matcher->size() > diff_at ? charToUint8((*matcher)[diff_at]) : static_cast<uint8_t>(0);
            auto direct = static_cast<bool>
            ((1 + (crit_byte | node->immut_masks()[min_it - node->immut_diffs().cbegin()])) >> 8);
            if (!direct) { // left
                cend = min_it;
            } else { // right
                cbegin = min_it + 1;
            }

            if (cbegin == cend) {
                return {min_it - node->immut_diffs().cbegin(), direct, size};
            }
        }
    }

    void BitDegradeTree::combatInsert(const Slice & opponent, const Slice & k, OffsetToData v) {
        std::unique_ptr<Matcher> opponent_m = sliceToMatcher(opponent);
        std::unique_ptr<Matcher> k_m = sliceToMatcher(k);

        uint32_t diff_at = 0;
        while ((*opponent_m)[diff_at] == (*k_m)[diff_at]) {
            ++diff_at;
        }

        uint8_t mask = charToUint8((*opponent_m)[diff_at] ^ (*k_m)[diff_at]);
        mask |= mask >> 1;
        mask |= mask >> 2;
        mask |= mask >> 4;
        mask = (mask & ~(mask >> 1)) ^ static_cast<uint8_t>(UINT8_MAX);
        auto direct = static_cast<bool>((1 + (mask | charToUint8((*k_m)[diff_at]))) >> 8);

        BDNode * cursor = offToMemNode(_root);
        while (true) {
            size_t replace_idx;
            bool replace_direct;
            size_t cursor_size = cursor->size();

            auto cmp = cursor->functor();
            const uint32_t * cbegin = cursor->immut_diffs().cbegin();
            const uint32_t * cend = cursor->immut_diffs().cbegin() + cursor_size - 1;
            const uint32_t * min_it = std::min_element(cbegin, cend, cmp);

            if (*min_it > diff_at
                || (*min_it == diff_at && cursor->immut_masks()[min_it - cursor->immut_diffs().cbegin()] > mask)) {
                if (!direct) { // left
                    replace_idx = 0;
                    replace_direct = false;
                } else { // right
                    replace_idx = cursor_size - 1 - 1;
                    replace_direct = true;
                }
            } else {
                if (cursor_size <= 1) {
                    replace_idx = 0;
                    replace_direct = false;
                } else {
                    while (true) {
                        uint32_t crit_diff_at = *min_it;
                        uint8_t crit_byte = k_m->size() > crit_diff_at ? charToUint8((*k_m)[crit_diff_at])
                                                                       : static_cast<uint8_t>(0);
                        auto crit_direct = static_cast<bool>
                        ((1 + (crit_byte | cursor->immut_masks()[min_it - cursor->immut_diffs().cbegin()])) >> 8);
                        if (!crit_direct) {
                            cend = min_it;
                        } else {
                            cbegin = min_it + 1;
                        }

                        if (cbegin == cend) {
                            replace_idx = min_it - cursor->immut_diffs().cbegin();
                            replace_direct = crit_direct;
                            break;
                        }

                        const uint32_t * next_it = std::min_element(cbegin, cend, cmp);
                        if (*next_it > diff_at
                            || (*next_it == diff_at
                                && cursor->immut_masks()[next_it - cursor->immut_diffs().cbegin()] > mask)) {
                            if (!direct) { // left
                                replace_idx = cbegin - cursor->immut_diffs().cbegin();
                                replace_direct = false;
                            } else { // right
                                replace_idx = cend - cursor->immut_diffs().cbegin() - 1;
                                replace_direct = true;
                            }
                            break;
                        }
                        min_it = next_it;
                    }
                }
            }

            CritPtr ptr = cursor->immut_ptrs()[replace_idx + static_cast<size_t>(replace_direct)];
            if (cursor->immut_diffs()[replace_idx] > diff_at
                || (cursor->immut_diffs()[replace_idx] == diff_at && cursor->immut_masks()[replace_idx] > mask)
                || ptr.isData()) {
                if (cursor->full()) {
                    auto off = static_cast<uint32_t>(reinterpret_cast<char *>(cursor) -
                                                     reinterpret_cast<char *>(_dst.immut_mmaped_region()));
                    makeRoom(cursor);
                    cursor = offToMemNode(OffsetToNode{off});
                    continue;
                }
                nodeInsert(cursor, replace_idx, replace_direct, direct, diff_at, mask, v, cursor_size);
                break;
            }
            cursor = offToMemNode(ptr.asNode());
        }
    }

#define add_gap(arr, idx, size) memmove(&(arr)[(idx) + 1], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define del_gap(arr, idx, size) memmove(&(arr)[(idx)], &(arr)[(idx) + 1], sizeof((arr)[0]) * ((size) - ((idx) + 1)))

    void BitDegradeTree::nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                                    bool direct, uint32_t diff_at, uint8_t mask, OffsetToData v,
                                    size_t size) noexcept {
        assert(!node->full());
        size_t ptr_idx;
        if (size == 1) {
            assert(replace_idx == 0);
            ptr_idx = static_cast<size_t>(direct);
        } else {
            if (!replace_direct) {
                ptr_idx = replace_idx + static_cast<size_t>(direct);
            } else {
                ++replace_idx;
                ptr_idx = replace_idx + static_cast<size_t>(direct);
            }
        }

        add_gap(node->mut_diffs(), replace_idx, size - 1);
        add_gap(node->mut_masks(), replace_idx, size - 1);
        add_gap(node->mut_ptrs(), ptr_idx, size);

        node->mut_diffs()[replace_idx] = diff_at;
        node->mut_masks()[replace_idx] = mask;
        node->mut_ptrs()[ptr_idx].setData(v);
        node->updateChecksum();
    }

    void BitDegradeTree::makeRoom(BDNode * parent) {
        auto cmp = parent->functor();
        size_t rnd = std::uniform_int_distribution<size_t>(0, parent->immut_ptrs().size() - 1)(_gen);

        size_t i = rnd;
        do {
            BDNode * node;
            if (parent->immut_ptrs()[i].isNode() && !(node = offToMemNode(parent->immut_ptrs()[i].asNode()))->full()) {
                // try left
                if (i != 0) {
                    size_t diff_idx = i - 1;
                    if (diff_idx < 1
                        || cmp(parent->immut_diffs()[diff_idx - 1], parent->immut_diffs()[diff_idx])) {
                        if (diff_idx + 1 > parent->immut_diffs().size() - 1
                            || cmp(parent->immut_diffs()[diff_idx + 1], parent->immut_diffs()[diff_idx])) {
                            makeRoomPush(parent, node, diff_idx, false);
                            break;
                        }
                    }
                }
                // try right
                if (i != parent->immut_ptrs().size() - 1) {
                    size_t diff_idx = i;
                    if (diff_idx < 1
                        || cmp(parent->immut_diffs()[diff_idx - 1], parent->immut_diffs()[diff_idx])) {
                        if (diff_idx + 1 > parent->immut_diffs().size() - 1
                            || cmp(parent->immut_diffs()[diff_idx + 1], parent->immut_diffs()[diff_idx])) {
                            makeRoomPush(parent, node, diff_idx, true);
                            break;
                        }
                    }
                }
            }

            if (++i == parent->immut_ptrs().size()) {
                i = 0;
            }
        } while (i != rnd);

        if (parent->full()) {
            const uint32_t * biggest = std::max_element(parent->immut_diffs().cbegin(),
                                                        parent->immut_diffs().cend(),
                                                        cmp);
            makeNewRoom(parent, biggest - parent->immut_diffs().cbegin());
        }
    }

    void BitDegradeTree::makeRoomPush(BDNode * parent, BDNode * child, size_t idx, bool direct) noexcept {
        size_t parent_size = parent->immut_ptrs().size();
        size_t child_size = child->size();

        if (!direct) { // merge left to front
            add_gap(child->mut_diffs(), 0, child_size - 1);
            add_gap(child->mut_masks(), 0, child_size - 1);
            add_gap(child->mut_ptrs(), 0, child_size);

            child->mut_diffs()[0] = parent->immut_diffs()[idx];
            child->mut_masks()[0] = parent->immut_masks()[idx];
            child->mut_ptrs()[0] = parent->immut_ptrs()[idx];

            del_gap(parent->mut_diffs(), idx, parent_size - 1);
            del_gap(parent->mut_masks(), idx, parent_size - 1);
            del_gap(parent->mut_ptrs(), idx, parent_size);
            parent->mut_ptrs()[parent_size - 1].setNull();
        } else { // merge right to back
            child->mut_diffs()[child_size - 1] = parent->immut_diffs()[idx];
            child->mut_masks()[child_size - 1] = parent->immut_masks()[idx];
            child->mut_ptrs()[child_size] = parent->immut_ptrs()[idx + 1];

            del_gap(parent->mut_diffs(), idx, parent_size - 1);
            del_gap(parent->mut_masks(), idx, parent_size - 1);
            del_gap(parent->mut_ptrs(), idx + 1, parent_size);
            parent->mut_ptrs()[parent_size - 1].setNull();
        }

        parent->updateChecksum();
        child->updateChecksum();
    }

    void BitDegradeTree::makeNewRoom(BDNode * parent, size_t idx) {
        auto off = static_cast<uint32_t>(reinterpret_cast<char *>(parent) -
                                         reinterpret_cast<char *>(_dst.immut_mmaped_region()));
        OffsetToNode offset = mallocNode();
        parent = offToMemNode({off});
        auto child = new(offToMemNodeUnchecked(offset)) BDNode;

        child->mut_diffs()[0] = parent->immut_diffs()[idx];
        child->mut_masks()[0] = parent->immut_masks()[idx];
        child->mut_ptrs()[0] = parent->immut_ptrs()[idx];
        child->mut_ptrs()[1] = parent->immut_ptrs()[idx + 1];

        size_t size = parent->immut_ptrs().size();
        del_gap(parent->mut_diffs(), idx, size - 1);
        del_gap(parent->mut_masks(), idx, size - 1);
        del_gap(parent->mut_ptrs(), idx + 1, size);
        parent->mut_ptrs()[size - 1].setNull();
        parent->mut_ptrs()[idx].setNode(offset);

        parent->updateChecksum();
        child->updateChecksum();
    }

    void BitDegradeTree::nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept {
        assert(size >= 1);
        if (size - 1 > idx + 1) {
            del_gap(node->mut_diffs(), idx, size - 1);
            del_gap(node->mut_masks(), idx, size - 1);
        }
        del_gap(node->mut_ptrs(), idx + direct, size);
        node->mut_ptrs()[size - 1].setNull();
        node->updateChecksum();
    }

#define add_n_gap(arr, idx, size, n) memmove(&(arr)[(idx) + (n)], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define cpy_all(dst, idx, src, size) memcpy(&(dst)[(idx)], &(src)[0], sizeof((src)[0]) * (size));

    void BitDegradeTree::tryMerge(BDNode * parent, BDNode * child,
                                  size_t idx, bool direct, size_t parent_size,
                                  size_t child_size) noexcept {
        size_t ptr_idx = idx + static_cast<size_t>(direct);
        if (child_size == 1) {
            OffsetToNode offset = parent->immut_ptrs()[ptr_idx].asNode();
            parent->mut_ptrs()[ptr_idx] = child->immut_ptrs()[0];
            parent->updateChecksum();
            freeNode(offset);
        } else {
            assert(child_size > 1);
            if (parent->immut_ptrs().size() - parent_size + 1 >= child_size) {
                OffsetToNode offset = parent->immut_ptrs()[ptr_idx].asNode();

                if (!direct) { // left
                    add_n_gap(parent->mut_diffs(), idx, parent_size - 1, child_size - 1);
                    add_n_gap(parent->mut_masks(), idx, parent_size - 1, child_size - 1);
                    add_n_gap(parent->mut_ptrs(), idx + 1, parent_size, child_size - 1);

                    cpy_all(parent->mut_diffs(), idx, child->immut_diffs(), child_size - 1);
                    cpy_all(parent->mut_masks(), idx, child->immut_masks(), child_size - 1);
                    cpy_all(parent->mut_ptrs(), idx, child->immut_ptrs(), child_size);
                } else { // right
                    add_n_gap(parent->mut_diffs(), idx + 1, parent_size - 1, child_size - 1);
                    add_n_gap(parent->mut_masks(), idx + 1, parent_size - 1, child_size - 1);
                    add_n_gap(parent->mut_ptrs(), idx + 1 + 1, parent_size, child_size - 1);

                    cpy_all(parent->mut_diffs(), idx + 1, child->immut_diffs(), child_size - 1);
                    cpy_all(parent->mut_masks(), idx + 1, child->immut_masks(), child_size - 1);
                    cpy_all(parent->mut_ptrs(), idx + 1, child->immut_ptrs(), child_size);
                }

                parent->updateChecksum();
                freeNode(offset);
            }
        }
    }
}