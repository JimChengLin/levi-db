#pragma once
#ifndef LEVIDB8_INDEX_HPP
#define LEVIDB8_INDEX_HPP

#include <algorithm>
#include <climits>
#include <cstring>

#include "../include/exception.h"
#include "index_internal.h"
#include "usr.h"

namespace levidb8 {
    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::BitDegradeTree(const std::string & fname)
            :_dst(fname), _node_locks(1) {
        assert(_dst.immut_length() == kPageSize);
        new(offToMemNode(_root)) BDNode;
        _node_locks[0] = std::make_unique<ReadWriteLock>();
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::BitDegradeTree(const std::string & fname, OffsetToEmpty empty)
            : _dst(fname), _empty(empty), _node_locks(_dst.immut_length() / kPageSize) {
        for (auto & l:_node_locks) {
            l = std::make_unique<ReadWriteLock>();
        }
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    OffsetToData BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::find(const Slice & k) const {
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

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    size_t BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::size() const {
        RWLockReadGuard read_guard(&_expand_lock);
        return size(_root);
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    size_t BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::size(OffsetToNode node) const {
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

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::insert(const Slice & k, OffsetToData v) {
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
                        break;
                    }
                    goto restart;
                }
                if (ptr.isNode()) {
                    OffsetToNode off = ptr.asNode();
                    node_read_guard = RWLockReadGuard(offToNodeLock(off));
                    cursor = offToMemNode(off);
                } else {
                    OFFSET_M matcher(ptr.asData(), _cache);
                    Slice exist = matcher.toSlice(usr);

                    if (k == exist) {
                        RWLockWriteGuard node_write_guard;
                        if (RWLockReadGuard::tryUpgrade(&node_read_guard, &node_write_guard)) {
                            if (ptr.isDataSpecial()) {
                                ptr.setData(v);
                                ptr.markDataSpecial();
                            } else {
                                ptr.setData(v);
                            }
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
                if (_dst.immut_length() + kPageSize > kIndexFileLimit) {
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

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::remove(const Slice & k, OffsetToData v) {
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
                OFFSET_M matcher(ptr.asData(), _cache);
                if (matcher == k) {
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
                    if (ptr.isDataSpecial() || matcher.isCompress()) {
                        UPGRADE_CHILD();
                        ptr.setData(v);
                        ptr.markDataSpecial();
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

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
    BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::
    findBestMatch(const BDNode * node, const Slice & k, USR * reveal_info) const noexcept {
        const uint16_t * cbegin = node->immut_diffs().cbegin();
        const uint16_t * cend;

        size_t size = node->size();
        if (size <= 1) {
            return {0, false, size};
        }
        cend = &node->immut_diffs()[size - 1];

        CritBitPyramid pyramid;
        SLICE_M matcher(k);
        const uint16_t * min_it = cbegin + pyramid.build(cbegin, cend);
        while (true) {
            assert(min_it == std::min_element(cbegin, cend));
            const uint16_t diff_at = getDiffAt(*min_it);
            const uint8_t shift = getShift(*min_it);
            const uint8_t mask = static_cast<uint8_t>(1) << shift;

            // left or right?
            uint8_t crit_byte = matcher.size() > diff_at ? charToUint8(matcher[diff_at]) : static_cast<uint8_t>(0);
            auto direct = static_cast<bool>((1 + (crit_byte | static_cast<uint8_t>(~mask))) >> 8);
            if (reveal_info != nullptr) {
                reveal_info->reveal(diff_at, uint8ToChar(mask), direct, shift);
            }

            if (!direct) { // left
                cend = min_it;
                if (cbegin == cend) {
                    return {min_it - node->immut_diffs().cbegin(), direct, size};
                }
                min_it = node->immut_diffs().cbegin() + pyramid.trimRight(node->immut_diffs().cbegin(), cbegin, cend);
            } else { // right
                cbegin = min_it + 1;
                if (cbegin == cend) {
                    return {min_it - node->immut_diffs().cbegin(), direct, size};
                }
                min_it = node->immut_diffs().cbegin() + pyramid.trimLeft(node->immut_diffs().cbegin(), cbegin, cend);
            }
        }
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    bool BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::
    combatInsert(const Slice & opponent, OffsetToData from, const Slice & k, OffsetToData v) {
        SLICE_M opponent_m(opponent);
        SLICE_M k_m(k);

        uint16_t diff_at = 0;
        while (opponent_m[diff_at] == k_m[diff_at]) {
            ++diff_at;
        }

        // __builtin_clz: returns the number of leading 0-bits in x, starting at the most significant bit position
        // if x is 0, the result is undefined
        uint8_t shift = CHAR_BIT * sizeof(unsigned int)
                        - __builtin_clz(charToUint8(opponent_m[diff_at] ^ k_m[diff_at])) - 1;
        auto mask = static_cast<uint8_t>(1 << shift);
        auto direct = static_cast<bool>((1 + (static_cast<uint8_t>(~mask) | charToUint8(k_m[diff_at]))) >> 8);

        uint16_t cmp = static_cast<uint16_t>(~shift & 0b111) | (diff_at << 3);
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
                const uint16_t * cbegin = cursor->immut_diffs().cbegin();
                const uint16_t * cend = cursor->immut_diffs().cbegin() + cursor_size - 1;

                CritBitPyramid pyramid;
                const uint16_t * min_it = cbegin + pyramid.build(cbegin, cend);
                while (true) {
                    assert(min_it == std::min_element(cbegin, cend));
                    const uint16_t crit_diff_at = getDiffAt(*min_it);
                    const uint8_t crit_shift = getShift(*min_it);
                    const uint8_t crit_mask = static_cast<uint8_t>(1) << crit_shift;

                    if (*min_it > cmp) {
                        if (!direct) { // left
                            replace_idx = cbegin - cursor->immut_diffs().cbegin();
                            replace_direct = false;
                        } else { // right
                            replace_idx = cend - cursor->immut_diffs().cbegin() - 1;
                            replace_direct = true;
                        }
                        break;
                    }

                    uint8_t crit_byte = k_m.size() > crit_diff_at ? charToUint8(k_m[crit_diff_at])
                                                                  : static_cast<uint8_t>(0);
                    auto crit_direct = static_cast<bool>((1 + (crit_byte | static_cast<uint8_t>(~crit_mask))) >> 8);
                    if (!crit_direct) { // left
                        cend = min_it;
                        if (cbegin == cend) {
                            replace_idx = min_it - cursor->immut_diffs().cbegin();
                            replace_direct = crit_direct;
                            break;
                        }
                        min_it = cursor->immut_diffs().cbegin() +
                                 pyramid.trimRight(cursor->immut_diffs().cbegin(), cbegin, cend);
                    } else { // right
                        cbegin = min_it + 1;
                        if (cbegin == cend) {
                            replace_idx = min_it - cursor->immut_diffs().cbegin();
                            replace_direct = crit_direct;
                            break;
                        }
                        min_it = cursor->immut_diffs().cbegin() +
                                 pyramid.trimLeft(cursor->immut_diffs().cbegin(), cbegin, cend);
                    }
                }
            }

            CritPtr ptr = cursor->immut_ptrs()[replace_idx + static_cast<size_t>(replace_direct)];
            if (cursor->immut_diffs()[replace_idx] > cmp || ptr.isData()) {
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
                        if (probe_ptr.isNull()) {
                            return false;
                        }
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
                nodeInsert(cursor, replace_idx, replace_direct, direct, cmp, v, cursor_size);
                break;
            }

            node_read_guard = RWLockReadGuard(offToNodeLock(ptr.asNode()));
            cursor = offToMemNode(ptr.asNode());
        }
        return true;
    }

#define add_gap(arr, idx, size) memmove(&(arr)[(idx) + 1], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define del_gap(arr, idx, size) memmove(&(arr)[(idx)], &(arr)[(idx) + 1], sizeof((arr)[0]) * ((size) - ((idx) + 1)))

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                                                              bool direct, uint16_t diff, OffsetToData v,
                                                              size_t size) noexcept {
        assert(!node->full());
        replace_idx += static_cast<size_t>(replace_direct);
        size_t ptr_idx = replace_idx + static_cast<size_t>(direct);

        add_gap(node->mut_diffs(), replace_idx, size - 1);
        add_gap(node->mut_ptrs(), ptr_idx, size);

        node->mut_diffs()[replace_idx] = diff;
        node->mut_ptrs()[ptr_idx].setData(v);
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::makeRoom(BDNode * parent) {
        auto off = static_cast<uint32_t>(reinterpret_cast<char *>(parent) -
                                         reinterpret_cast<char *>(_dst.immut_mmaped_region()));
        OffsetToNode offset = mallocNode();
        parent = offToMemNode({off});
        auto child = new(offToMemNode(offset)) BDNode;

        // find nearly half
        const uint16_t * cbegin = parent->immut_diffs().cbegin();
        const uint16_t * cend = parent->immut_diffs().cend();

        CritBitPyramid pyramid;
        const uint16_t * min_it = cbegin + pyramid.build(cbegin, cend);
        while (true) {
            assert(min_it == std::min_element(cbegin, cend));
            if (min_it - cbegin < cend - min_it) { // right
                cbegin = min_it + 1;
                if (cend - cbegin <= parent->immut_diffs().size() / 2) {
                    break;
                }
                min_it = parent->immut_diffs().cbegin() +
                         pyramid.trimLeft(parent->immut_diffs().cbegin(), cbegin, cend);
            } else { // left
                cend = min_it;
                if (cend - cbegin <= parent->immut_diffs().size() / 2) {
                    break;
                }
                min_it = parent->immut_diffs().cbegin() +
                         pyramid.trimRight(parent->immut_diffs().cbegin(), cbegin, cend);
            }
        }

        const size_t item_num = cend - cbegin;
        const size_t nth = cbegin - parent->immut_diffs().cbegin();

        static constexpr size_t diff_size = sizeof(parent->immut_diffs()[0]);
        static constexpr size_t ptr_size = sizeof(parent->immut_ptrs()[0]);

        memcpy(child->mut_diffs().begin(), &parent->immut_diffs()[nth], diff_size * item_num);
        memcpy(child->mut_ptrs().begin(), &parent->immut_ptrs()[nth], ptr_size * (item_num + 1));

        size_t left = parent->immut_diffs().cend() - &parent->immut_diffs()[nth + item_num];
        memmove(&parent->mut_diffs()[nth], &parent->immut_diffs()[nth] + item_num, diff_size * left);
        memmove(&parent->mut_ptrs()[nth + 1], &parent->immut_ptrs()[nth] + item_num + 1, ptr_size * left);

        parent->mut_ptrs()[nth].setNode(offset);
        memset(parent->mut_ptrs().end() - item_num, -1/* all 1 */, ptr_size * item_num);
        assert(parent->size() == parent->immut_ptrs().size() - item_num);
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::
    nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept {
        assert(size >= 1);
        if (size > 1) {
            del_gap(node->mut_diffs(), idx, size - 1);
        }
        del_gap(node->mut_ptrs(), idx + direct, size);
        node->mut_ptrs()[size - 1].setNull();
    }

#define add_n_gap(arr, idx, size, n) memmove(&(arr)[(idx) + (n)], &(arr)[(idx)], sizeof((arr)[0]) * ((size) - (idx)))
#define cpy_all(dst, idx, src, size) memcpy(&(dst)[(idx)], &(src)[0], sizeof((src)[0]) * (size));

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::
    tryMerge(BDNode * parent, size_t idx, bool direct, size_t parent_size,
             BDNode * child, size_t child_size) noexcept {
        size_t ptr_idx = idx + static_cast<size_t>(direct);
        assert(child_size > 1);
        if (parent->immut_ptrs().size() - parent_size + 1 >= child_size) {
            OffsetToNode offset = parent->immut_ptrs()[ptr_idx].asNode();
            idx += static_cast<size_t>(direct);

            add_n_gap(parent->mut_diffs(), idx, parent_size - 1, child_size - 1);
            add_n_gap(parent->mut_ptrs(), idx + 1, parent_size, child_size - 1);

            cpy_all(parent->mut_diffs(), idx, child->immut_diffs(), child_size - 1);
            cpy_all(parent->mut_ptrs(), idx, child->immut_ptrs(), child_size);

            freeNode(offset);
        }
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    BDNode * BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::offToMemNode(OffsetToNode node) const noexcept {
        return reinterpret_cast<BDNode *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region()) + node.val);
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    ReadWriteLock * BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::offToNodeLock(OffsetToNode node) const noexcept {
        assert(_dst.immut_length() == _node_locks.size() * kPageSize);
        return _node_locks[node.val / kPageSize].get();
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    BDEmpty * BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::offToMemEmpty(OffsetToEmpty empty) const {
        auto * mem_empty = reinterpret_cast<BDEmpty *>(reinterpret_cast<uintptr_t>(_dst.immut_mmaped_region())
                                                       + empty.val);
        if (!mem_empty->verify()) {
            throw Exception::corruptionException("e-node checksum mismatch");
        };
        return mem_empty;
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    OffsetToNode BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::mallocNode() {
        std::lock_guard<std::mutex> guard(_allocate_lock);
        if (_empty.val == kDiskNull) {
            throw ExpandControlledException();
        }
        OffsetToNode res{_empty.val};
        _empty = offToMemEmpty(_empty)->immut_next();
        return res;
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::freeNode(OffsetToNode node) noexcept {
        std::lock_guard<std::mutex> guard(_allocate_lock);
        freeNodeUnlocked(node);
    }

    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    void BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::freeNodeUnlocked(OffsetToNode node) noexcept {
        auto empty = new(offToMemNode(node)) BDEmpty;
        empty->mut_next() = _empty;
        empty->updateChecksum();
        _empty.val = node.val;
    }
}

#endif //LEVIDB8_INDEX_HPP
