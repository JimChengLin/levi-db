#pragma once
#ifndef LEVIDB8_INDEX_SCAN_HPP
#define LEVIDB8_INDEX_SCAN_HPP

#include <algorithm>

#include "index_internal.h"
#include "usr.h"

namespace levidb8 {
    template<typename OFFSET_M, typename SLICE_M, typename CACHE>
    class BitDegradeTree<OFFSET_M, SLICE_M, CACHE>::BDIterator {
    private:
        const BitDegradeTree * _index;
        USR _usr;
        std::string _key;
        BDNode _node_clone;
        const CritBitNode * _head{};
        std::array<CritBitNode, kRank + 1> _crit_nodes;
        int _cursor{};
        bool _valid = false;

    public:
        // coverity[uninit_member]
        explicit BDIterator(const BitDegradeTree * index) noexcept : _index(index) {}

        bool valid() const noexcept {
            return _valid;
        }

        void seekToFirst() noexcept {
            loadToLeftest();
        }

        void seekToLast() noexcept {
            loadToRightest();
        }

        void seek(const Slice & k) noexcept {
            loadToKey(k);
        }

        void next() noexcept {
            assert(valid());
            if ((++_cursor) >= _node_clone.immut_ptrs().size()
                || _node_clone.immut_ptrs()[_cursor].isNull()
                || _node_clone.immut_ptrs()[_cursor].isNode()) {
                reloadToRight();
            } else {
                configureUsrForNext();
            }
        }

        void prev() noexcept {
            assert(valid());
            if ((--_cursor) < 0
                || _node_clone.immut_ptrs()[_cursor].isNull()
                || _node_clone.immut_ptrs()[_cursor].isNode()) {
                reloadToLeft();
            } else {
                configureUsrForPrev();
            }
        }

        Slice key() const noexcept {
            assert(valid());
            return _usr.toSlice();
        }

        OffsetToData value() const noexcept {
            assert(valid());
            return _node_clone.immut_ptrs()[_cursor].asData();
        }

    private:
        void loadToLeftest() {
            loadToTarget<false>({});
        }

        void loadToRightest() {
            loadToTarget<true>({});
        }

        void loadToKey(const Slice & k) {
            _usr.clear();
            {
                RWLockReadGuard read_guard(&_index->_expand_lock);
                RWLockReadGuard node_read_guard(_index->offToNodeLock(_root));
                const BDNode * cursor = _index->offToMemNode(_root);
                while (true) {
                    auto pos = _index->findBestMatch(cursor, k, &_usr);
                    size_t idx;
                    bool direct;
                    std::tie(idx, direct, std::ignore) = pos;

                    _cursor = static_cast<int>(idx + direct);
                    CritPtr ptr = cursor->immut_ptrs()[_cursor];
                    if (ptr.isNull()) {
                        _valid = false;
                        return;
                    }
                    if (ptr.isNode()) {
                        node_read_guard = RWLockReadGuard(_index->offToNodeLock(ptr.asNode()));
                        cursor = _index->offToMemNode(ptr.asNode());
                    } else {
                        _node_clone = *cursor;
                        _valid = true;
                        break;
                    }
                }
            }

            size_t _;
            _head = parseBDNode(&_node_clone, _, _crit_nodes);
        }

        void reloadToRight() {
            if (metMax()) {
                _valid = false;
            } else {
                loadToTarget<false>(largerKey());
            }
        }

        void reloadToLeft() {
            if (metMin()) {
                _valid = false;
            } else {
                loadToTarget<true>(smallerKey());
            }
        }

        void configureUsrForNext() noexcept {
            configureUsr<false>(largerKey());
        }

        void configureUsrForPrev() noexcept {
            configureUsr<true>(smallerKey());
        }

    private:
        template<bool RIGHT_FIRST>
        std::pair<size_t, bool>
        findBestMatch(const BDNode * node, const Slice & target, USR * reveal_info) noexcept {
            const uint16_t * cbegin = node->immut_diffs().cbegin();
            const uint16_t * cend;

            size_t size = node->size();
            if (size <= 1) {
                return {0, false};
            }
            cend = &node->immut_diffs()[size - 1];

            CritBitPyramid pyramid;
            const uint16_t * min_it = cbegin + pyramid.build(cbegin, cend);
            while (true) {
                assert(min_it == std::min_element(cbegin, cend));
                const uint16_t diff_at = getDiffAt(*min_it);
                const uint8_t shift = getShift(*min_it);
                const uint8_t mask = static_cast<uint8_t>(1) << shift;

                // left or right?
                uint8_t crit_byte = target.size() > diff_at ? charToUint8(target[diff_at])
                                                            : static_cast<uint8_t>(RIGHT_FIRST ? UINT8_MAX : 0);
                auto direct = static_cast<bool>((1 + (crit_byte | static_cast<uint8_t>(~mask))) >> 8);
                reveal_info->reveal(diff_at, uint8ToChar(mask), direct, getShift(*min_it));

                if (!direct) { // left
                    cend = min_it;
                    if (cbegin == cend) {
                        return {min_it - node->immut_diffs().cbegin(), direct};
                    }
                    min_it = node->immut_diffs().cbegin() +
                             pyramid.trimRight(node->immut_diffs().cbegin(), cbegin, cend);
                } else { // right
                    cbegin = min_it + 1;
                    if (cbegin == cend) {
                        return {min_it - node->immut_diffs().cbegin(), direct};
                    }
                    min_it = node->immut_diffs().cbegin() +
                             pyramid.trimLeft(node->immut_diffs().cbegin(), cbegin, cend);
                }
            }
        };

        template<bool RIGHT_FIRST>
        void loadToTarget(const Slice & target) {
            _usr.clear();
            {
                RWLockReadGuard read_guard(&_index->_expand_lock);
                RWLockReadGuard node_read_guard(_index->offToNodeLock(_root));
                const BDNode * cursor = _index->offToMemNode(_root);
                while (true) {
                    auto pos = findBestMatch<RIGHT_FIRST>(cursor, target, &_usr);
                    size_t idx;
                    bool direct;
                    std::tie(idx, direct) = pos;

                    _cursor = static_cast<int>(idx + direct);
                    CritPtr ptr = cursor->immut_ptrs()[_cursor];
                    if (ptr.isNull()) {
                        _valid = false;
                        return;
                    }
                    if (ptr.isNode()) {
                        node_read_guard = RWLockReadGuard(_index->offToNodeLock(ptr.asNode()));
                        cursor = _index->offToMemNode(ptr.asNode());
                    } else {
                        _node_clone = *cursor;
                        _valid = true;
                        break;
                    }
                }
            }

            size_t _;
            _head = parseBDNode(&_node_clone, _, _crit_nodes);
        }

        template<bool RIGHT_FIRST>
        void configureUsr(const Slice & key) noexcept {
            const CritBitNode * cursor = _head;
            while (true) {
                const uint16_t min_val = _node_clone.immut_diffs()[cursor - _crit_nodes.cbegin()];
                const uint16_t diff_at = getDiffAt(min_val);
                const uint8_t shift = getShift(min_val);
                const uint8_t mask = static_cast<uint8_t>(1) << shift;

                // left or right?
                uint8_t crit_byte = key.size() > diff_at ? charToUint8(key[diff_at])
                                                         : static_cast<uint8_t>(RIGHT_FIRST ? UINT8_MAX : 0);
                auto direct = static_cast<bool>((1 + (crit_byte | static_cast<uint8_t>(~mask))) >> 8);
                _usr.reveal(diff_at, uint8ToChar(mask), direct, shift);

                if (!direct) { // left
                    if (cursor->left != UINT16_MAX) {
                        cursor = &_crit_nodes[cursor->left];
                        continue;
                    }
                } else { // right
                    if (cursor->right != UINT16_MAX) {
                        cursor = &_crit_nodes[cursor->right];
                        continue;
                    }
                }
                assert(cursor + direct - _crit_nodes.cbegin() == _cursor);
                break;
            }
        }

        Slice smallerKey() noexcept {
            size_t i = _usr.immut_src().size();
            do {
                --i;
                if ((_usr.immut_extra()[i] & _usr.immut_src()[i]) == 0) {
                    assert(i != 0);
                    continue;
                }

                _key.resize(i + 1);
                memcpy(&_key[0], _usr.immut_src().data(), _key.size());

                --_key[i];
                for (size_t j = 0; j < _key.size(); ++j) {
                    _key[j] |= ~_usr.immut_extra()[j];
                }
                break;
            } while (true);
            return _key;
        }

        Slice largerKey() noexcept {
            size_t i = _usr.immut_src().size();
            do {
                --i;
                char xor_res;
                if ((xor_res = (_usr.immut_extra()[i] ^ _usr.immut_src()[i])) == 0) {
                    assert(i != 0);
                    continue;
                }

                _key.resize(i + 1);
                memcpy(&_key[0], _usr.immut_src().data(), _key.size());

                auto n = __builtin_ffs(xor_res) - 1;
                _key[i] |= 1 << n; // 0 to 1
                _key[i] &= uint8ToChar(UINT8_MAX << n);
                break;
            } while (true);
            return _key;
        }

        bool metMin() const noexcept {
            return std::all_of(_usr.immut_src().cbegin(), _usr.immut_src().cend(), [](char a) noexcept {
                return a == 0;
            });
        }

        bool metMax() const noexcept {
            return std::all_of(_usr.immut_src().cbegin(), _usr.immut_src().cend(), [this](const char & a) noexcept {
                return (a ^ _usr.immut_extra()[&a - &_usr.immut_src()[0]]) == 0;
            });
        }
    };
}

#endif //LEVIDB8_INDEX_SCAN_HPP
