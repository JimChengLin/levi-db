#ifndef __clang__

#include <climits>

#endif

#include "index_internal.h"
#include "usr.h"

namespace levidb8 {
    static constexpr OffsetToNode _root{0};

    BitDegradeTree::BitDegradeTree(const std::string & fname) : _dst(fname), _node_locks(1) {
        assert(_dst.immut_length() == kPageSize);
        new(offToMemNodeUnchecked(_root)) BDNode;
        _node_locks[0] = std::make_unique<ReadWriteLock>();
    }

    BitDegradeTree::BitDegradeTree(const std::string & fname, OffsetToEmpty empty)
            : _dst(fname), _empty(empty), _node_locks(_dst.immut_length() / kPageSize) {
        for (auto & l:_node_locks) {
            l = std::make_unique<ReadWriteLock>();
        }
    }

    OffsetToData BitDegradeTree::find(const Slice & k) const {
        RWLockReadGuard read_guard(&_expand_lock);
        RWLockReadGuard node_read_guard(offToNodeLock(_root));
        const BDNode * cursor = offToMemNode(_root);

        while (true) {
            auto pos = findBestMatch(cursor, k);
            size_t idx;
            bool direct;
            std::tie(idx, direct, std::ignore) = pos;

            CritPtr ptr = cursor->immut_ptrs()[idx + static_cast<size_t>(direct)];
            if (ptr.isNull()) {
                return {kDiskNull};
            }
            if (ptr.isNode()) {
                node_read_guard = RWLockReadGuard(offToNodeLock(ptr.asNode()));
                cursor = offToMemNode(ptr.asNode());
            } else {
                return ptr.asData();
            }
        }
    }

    size_t BitDegradeTree::size() const {
        RWLockReadGuard read_guard(&_expand_lock);
        return size(_root);
    }

    size_t BitDegradeTree::size(OffsetToNode node) const {
        size_t cnt = 0;
        RWLockReadGuard node_read_guard(offToNodeLock(node));
        const BDNode * mem_node = offToMemNode(node);
        for (CritPtr ptr:mem_node->immut_ptrs()) {
            if (ptr.isNull()) {
                break;
            }
            if (ptr.isData()) {
                ++cnt;
            } else {
                cnt += size(ptr.asNode());
            }
        }
        return cnt;
    }

    void BitDegradeTree::insert(const Slice & k, OffsetToData v) {
        restart:
        try {
            RWLockReadGuard read_guard(&_expand_lock);
            RWLockReadGuard node_read_guard(offToNodeLock(_root));
            BDNode * cursor = offToMemNode(_root);

            USR usr;
            while (true) {
                auto pos = findBestMatch(cursor, k, &usr);
                size_t idx;
                bool direct;
                std::tie(idx, direct, std::ignore) = pos;

                CritPtr & ptr = cursor->mut_ptrs()[idx + static_cast<size_t>(direct)];
                if (ptr.isNull()) {
                    RWLockWriteGuard node_write_guard;
                    if (RWLockReadGuard::tryUpgrade(&node_read_guard, &node_write_guard)) {
                        ptr.setData(v);
                        cursor->setSize(1);
                        break;
                    }
                    goto restart;
                }
                if (ptr.isNode()) {
                    OffsetToNode off = ptr.asNode();
                    node_read_guard = RWLockReadGuard(offToNodeLock(off));
                    cursor = offToMemNode(off);
                } else {
                    std::unique_ptr<Matcher> matcher = offToMatcher(ptr.asData());
                    Slice exist = matcher->toSlice(usr.toSlice());

                    if (k == exist) {
                        RWLockWriteGuard node_write_guard;
                        if (RWLockReadGuard::tryUpgrade(&node_read_guard, &node_write_guard)) {
                            ptr.setData(v);
                        } else {
                            goto restart;
                        }
                    } else {
                        OffsetToData off = ptr.asData();
                        node_read_guard.release();
                        if (!combatInsert(exist, off, k, v)) {
                            goto restart;
                        }
                    }
                    break;
                }
            }
        } catch (const ExpandControlledException &) {
            RWLockWriteGuard write_guard(&_expand_lock);
            if (_empty.val == kDiskNull) {
                if (_dst.immut_length() + kPageSize > kFileAddressLimit) {
                    throw IndexFullControlledException();
                }
                auto cursor = static_cast<uint32_t>(_dst.immut_length());
                _dst.grow();
                _node_locks.resize(_dst.immut_length() / kPageSize);
                for (auto lock_cursor = _node_locks.end(); cursor < _dst.immut_length(); cursor += kPageSize) {
                    *(--lock_cursor) = std::make_unique<ReadWriteLock>();
                    freeNodeUnlocked(OffsetToNode{cursor});
                }
            }
            goto restart;
        }
    }

    void BitDegradeTree::remove(const Slice & k, OffsetToData v) {
        restart:
        RWLockReadGuard read_guard(&_expand_lock);
        RWLockReadGuard parent_read_guard;
        BDNode * parent = nullptr;
        RWLockReadGuard node_read_guard(offToNodeLock(_root));
        BDNode * cursor = offToMemNode(_root);
        size_t parent_idx{};
        bool parent_direct{};
        size_t parent_size{};

        while (true) {
            auto pos = findBestMatch(cursor, k);
            size_t idx;
            bool direct;
            size_t size;
            std::tie(idx, direct, size) = pos;

            CritPtr & ptr = cursor->mut_ptrs()[idx + static_cast<size_t>(direct)];
            if (ptr.isNull()) {
                break;
            }
            if (ptr.isNode()) {
                parent_read_guard = std::move(node_read_guard);
                parent = cursor;
                parent_idx = idx;
                parent_direct = direct;
                parent_size = size;
                node_read_guard = RWLockReadGuard(offToNodeLock(ptr.asNode()));
                cursor = offToMemNode(ptr.asNode());
            } else {
                std::unique_ptr<Matcher> matcher = offToMatcher(ptr.asData());
                if (*matcher == k) {
#define UPGRADE_CHILD() \
RWLockWriteGuard node_write_guard; \
if (!RWLockReadGuard::tryUpgrade(&node_read_guard, &node_write_guard)) { \
    goto restart; \
}
#define UPGRADE_PARENT() \
RWLockWriteGuard parent_write_guard; \
if (parent != nullptr \
    && !RWLockReadGuard::tryUpgrade(&parent_read_guard, &parent_write_guard)) { \
    goto restart; \
}
                    if (isSpecialMask(cursor->immut_masks()[idx])) {
                        UPGRADE_CHILD();
                        ptr.setData(v);
                    } else if (matcher->size() == 0) { // compress record case
                        UPGRADE_CHILD();
                        letMaskSpecial(cursor->mut_masks()[idx]);
                        ptr.setData(v);
                    } else {
                        UPGRADE_PARENT();
                        UPGRADE_CHILD();
                        nodeRemove(cursor, idx, direct, size);
                        if (parent != nullptr) {
                            tryMerge(parent, parent_idx, parent_direct, parent_size,
                                     cursor, size - 1);
                        }
                    }
                }
                break;
            }
        }
    }

    std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
    BitDegradeTree::findBestMatch(const BDNode * node, const Slice & k, USR * reveal_info) const noexcept {
        const uint32_t * cbegin = node->immut_diffs().cbegin();
        const uint32_t * cend;

        size_t size = node->size();
        if (size <= 1) {
            if (reveal_info != nullptr) {
                reveal_info->mut_src().resize(1);
                reveal_info->mut_src().front() = 0;
                reveal_info->mut_extra().resize(1);
                reveal_info->mut_extra().front() = 0;
            }
            return {0, false, size};
        }
        cend = &node->immut_diffs()[size - 1];

        bool use_cache = true;
        std::array<uint32_t, kRank> calc_cache{};
        std::unique_ptr<Matcher> matcher = sliceToMatcher(k);
        while (true) {
            const uint32_t * min_it = use_cache ? cbegin + node->minAt()
                                                : unfairMinElem(cbegin, cend, node, calc_cache);
            cheat:
            uint32_t diff_at = *min_it;
            uint8_t trans_mask = transMask(node->immut_masks()[min_it - node->immut_diffs().cbegin()]);

            // left or right?
            uint8_t crit_byte = matcher->size() > diff_at ? charToUint8((*matcher)[diff_at]) : static_cast<uint8_t>(0);
            auto direct = static_cast<bool>((1 + (crit_byte | trans_mask)) >> 8);
            if (reveal_info != nullptr) {
                reveal_info->reveal(diff_at, uint8ToChar(trans_mask), direct);
            }
            if (!direct) { // left
                cend = min_it;
            } else { // right
                cbegin = min_it + 1;
            }

            if (cbegin == cend) {
                return {min_it - node->immut_diffs().cbegin(), direct, size};
            }
            if (cend == min_it && !use_cache) {
                min_it = cbegin + calc_cache[min_it - cbegin - 1];
                goto cheat;
            }
            use_cache = false;
        }
    }

    bool BitDegradeTree::combatInsert(const Slice & opponent, OffsetToData from, const Slice & k, OffsetToData v) {
        std::unique_ptr<Matcher> opponent_m = sliceToMatcher(opponent);
        std::unique_ptr<Matcher> k_m = sliceToMatcher(k);

        uint32_t diff_at = 0;
        while ((*opponent_m)[diff_at] == (*k_m)[diff_at]) {
            ++diff_at;
        }

        uint8_t mask = charToUint8((*opponent_m)[diff_at] ^ (*k_m)[diff_at]);
        // __builtin_clz: returns the number of leading 0-bits in x, starting at the most significant bit position
        // if x is 0, the result is undefined
        mask = CHAR_BIT * sizeof(unsigned int) - __builtin_clz(mask) - 1;
        uint8_t trans_mask = transMask(mask);
        auto direct = static_cast<bool>((1 + (trans_mask | charToUint8((*k_m)[diff_at]))) >> 8);

        uint64_t cmp = mixMarks(diff_at, mask);
        RWLockReadGuard node_read_guard(offToNodeLock(_root));
        BDNode * cursor = offToMemNode(_root);
        while (true) {
            size_t replace_idx;
            bool replace_direct;
            size_t cursor_size = cursor->size();

            if (cursor_size <= 1) {
                replace_idx = 0;
                replace_direct = false;
            } else {
                const uint32_t * cbegin = cursor->immut_diffs().cbegin();
                const uint32_t * cend = cursor->immut_diffs().cbegin() + cursor_size - 1;

                bool use_cache = true;
                std::array<uint32_t, kRank> calc_cache{};
                while (true) {
                    const uint32_t * min_it = use_cache ? cbegin + cursor->minAt()
                                                        : unfairMinElem(cbegin, cend, cursor, calc_cache);
                    cheat:
                    uint32_t crit_diff_at = *min_it;
                    uint8_t crit_mask = cursor->immut_masks()[min_it - cursor->immut_diffs().cbegin()];

                    if (mixMarks(crit_diff_at, crit_mask) > cmp) {
                        if (!direct) { // left
                            replace_idx = cbegin - cursor->immut_diffs().cbegin();
                            replace_direct = false;
                        } else { // right
                            replace_idx = cend - cursor->immut_diffs().cbegin() - 1;
                            replace_direct = true;
                        }
                        break;
                    }

                    uint8_t crit_trans_mask = transMask(crit_mask);
                    uint8_t crit_byte = k_m->size() > crit_diff_at ? charToUint8((*k_m)[crit_diff_at])
                                                                   : static_cast<uint8_t>(0);
                    auto crit_direct = static_cast<bool>((1 + (crit_byte | crit_trans_mask)) >> 8);
                    if (!crit_direct) { // left
                        cend = min_it;
                    } else { // right
                        cbegin = min_it + 1;
                    }

                    if (cbegin == cend) {
                        replace_idx = min_it - cursor->immut_diffs().cbegin();
                        replace_direct = crit_direct;
                        break;
                    }
                    if (cend == min_it && !use_cache) {
                        min_it = cbegin + calc_cache[min_it - cbegin - 1];
                        goto cheat;
                    }
                    use_cache = false;
                }
            }

            CritPtr ptr = cursor->immut_ptrs()[replace_idx + static_cast<size_t>(replace_direct)];
            if (mixMarks(cursor->immut_diffs()[replace_idx], cursor->immut_masks()[replace_idx]) > cmp
                || ptr.isData()) {
                RWLockWriteGuard node_write_guard;
                if (!RWLockReadGuard::tryUpgrade(&node_read_guard, &node_write_guard)) {
                    return false;
                }

                if (cursor->full()) {
                    makeRoom(cursor); // may throw ExpandControlledException
                    RWLockWriteGuard::degrade(&node_write_guard, &node_read_guard);
                    continue;
                }
                // 验证前缀条件是否仍然成立
                {
                    RWLockReadGuard probe_read_guard;
                    const BDNode * probe = cursor;

                    while (true) {
                        auto pos = findBestMatch(probe, k);
                        size_t probe_idx;
                        bool probe_direct;
                        std::tie(probe_idx, probe_direct, std::ignore) = pos;

                        CritPtr probe_ptr = probe->immut_ptrs()[probe_idx + static_cast<size_t>(probe_direct)];
                        if (probe_ptr.isNode()) {
                            probe_read_guard = RWLockReadGuard(offToNodeLock(probe_ptr.asNode()));
                            probe = offToMemNode(probe_ptr.asNode());
                        } else {
                            if (probe_ptr.asData().val != from.val) {
                                return false;
                            }
                            break;
                        }
                    }
                }
                nodeInsert(cursor, replace_idx, replace_direct, direct, diff_at, mask, v, cursor_size);
                break;
            }

            node_read_guard = RWLockReadGuard(offToNodeLock(ptr.asNode()));
            cursor = offToMemNode(ptr.asNode());
        }
        return true;
    }

#define add_gap(arr, idx, size) memmove(&(arr)[(idx) + 1], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define del_gap(arr, idx, size) memmove(&(arr)[(idx)], &(arr)[(idx) + 1], sizeof((arr)[0]) * ((size) - ((idx) + 1)))

    void BitDegradeTree::nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                                    bool direct, uint32_t diff_at, uint8_t mask, OffsetToData v,
                                    size_t size) noexcept {
        assert(!node->full());
        replace_idx += static_cast<size_t>(replace_direct);
        size_t ptr_idx = replace_idx + static_cast<size_t>(direct);

        add_gap(node->mut_diffs(), replace_idx, size - 1);
        add_gap(node->mut_masks(), replace_idx, size - 1);
        add_gap(node->mut_ptrs(), ptr_idx, size);

        node->mut_diffs()[replace_idx] = diff_at;
        node->mut_masks()[replace_idx] = mask;
        node->mut_ptrs()[ptr_idx].setData(v);

        node->setSize(static_cast<uint16_t>(size + 1));
        node->setMinAt(static_cast<uint16_t>(node->calcMinAt()));
    }

    void BitDegradeTree::makeRoom(BDNode * parent) {
        auto off = static_cast<uint32_t>(reinterpret_cast<char *>(parent) -
                                         reinterpret_cast<char *>(_dst.immut_mmaped_region()));
        OffsetToNode offset = mallocNode();
        parent = offToMemNode({off});
        auto child = new(offToMemNodeUnchecked(offset)) BDNode;

        // find nearly half
        const uint32_t * cbegin = parent->immut_diffs().cbegin();
        const uint32_t * cend = parent->immut_diffs().cend();

        bool use_cache = true;
        std::array<uint32_t, kRank> calc_cache{};
        while (true) {
            const uint32_t * min_it = use_cache ? cbegin + parent->minAt()
                                                : unfairMinElem(cbegin, cend, parent, calc_cache);
            cheat:
            if (min_it - cbegin < cend - min_it) { // right
                cbegin = min_it + 1;
            } else { // left
                cend = min_it;
            }

            if (cend - cbegin <= parent->immut_diffs().size() / 2) {
                break;
            }
            if (cend == min_it && !use_cache) {
                min_it = cbegin + calc_cache[min_it - cbegin - 1];
                goto cheat;
            }
            use_cache = false;
        }

        size_t item_num = cend - cbegin;
        size_t nth = cbegin - parent->immut_diffs().cbegin();

        static constexpr size_t diff_size = sizeof(parent->immut_diffs()[0]);
        static constexpr size_t mask_size = sizeof(parent->immut_masks()[0]);
        static constexpr size_t ptr_size = sizeof(parent->immut_ptrs()[0]);

        memcpy(child->mut_diffs().begin(), &parent->immut_diffs()[nth], diff_size * item_num);
        memcpy(child->mut_masks().begin(), &parent->immut_masks()[nth], mask_size * item_num);
        memcpy(child->mut_ptrs().begin(), &parent->immut_ptrs()[nth], ptr_size * (item_num + 1));

        size_t left = parent->immut_diffs().cend() - (&parent->immut_diffs()[nth] + item_num);
        memmove(&parent->mut_diffs()[nth], &parent->immut_diffs()[nth] + item_num, diff_size * left);
        memmove(&parent->mut_masks()[nth], &parent->immut_masks()[nth] + item_num, mask_size * left);
        memmove(&parent->mut_ptrs()[nth + 1], &parent->immut_ptrs()[nth] + item_num + 1, ptr_size * left);

        parent->mut_ptrs()[nth].setNode(offset);
        memset(parent->mut_ptrs().end() - item_num, -1/* all 1 */, ptr_size * item_num);

        parent->update();
        child->update();
        assert(parent->size() == parent->immut_ptrs().size() - item_num);
    }

    void BitDegradeTree::nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept {
        assert(size >= 1);
        if (size > 1) {
            del_gap(node->mut_diffs(), idx, size - 1);
            del_gap(node->mut_masks(), idx, size - 1);
        }
        del_gap(node->mut_ptrs(), idx + direct, size);
        node->mut_ptrs()[size - 1].setNull();

        node->setSize(static_cast<uint16_t>(size - 1));
        node->setMinAt(static_cast<uint16_t>(node->calcMinAt()));
    }

#define add_n_gap(arr, idx, size, n) memmove(&(arr)[(idx) + (n)], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define cpy_all(dst, idx, src, size) memcpy(&(dst)[(idx)], &(src)[0], sizeof((src)[0]) * (size));

    void BitDegradeTree::tryMerge(BDNode * parent, size_t idx, bool direct, size_t parent_size,
                                  BDNode * child, size_t child_size) noexcept {
        size_t ptr_idx = idx + static_cast<size_t>(direct);
        if (child_size == 1) {
            OffsetToNode offset = parent->immut_ptrs()[ptr_idx].asNode();
            parent->mut_ptrs()[ptr_idx] = child->immut_ptrs()[0];
            freeNode(offset);
        } else {
            assert(child_size > 1);
            if (parent->immut_ptrs().size() - parent_size + 1 >= child_size) {
                OffsetToNode offset = parent->immut_ptrs()[ptr_idx].asNode();
                idx += static_cast<size_t>(direct);

                add_n_gap(parent->mut_diffs(), idx, parent_size - 1, child_size - 1);
                add_n_gap(parent->mut_masks(), idx, parent_size - 1, child_size - 1);
                add_n_gap(parent->mut_ptrs(), idx + 1, parent_size, child_size - 1);

                cpy_all(parent->mut_diffs(), idx, child->immut_diffs(), child_size - 1);
                cpy_all(parent->mut_masks(), idx, child->immut_masks(), child_size - 1);
                cpy_all(parent->mut_ptrs(), idx, child->immut_ptrs(), child_size);

                parent->update();
                freeNode(offset);
            }
        }
    }
}