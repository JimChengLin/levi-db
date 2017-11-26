#ifndef __clang__

#include <algorithm>

#endif

#include "index_internal.h"
#include "usr.h"

namespace levidb8 {
    static constexpr OffsetToNode _root{0};

    class BitDegradeTree::BitDegradeTreeIterator : public Iterator<Slice, OffsetToData> {
    private:
        const BitDegradeTree * _index;
        USR _usr;
        BDNode _node_clone;
        int _cursor{};
        bool _valid = false;

    public:
        explicit BitDegradeTreeIterator(const BitDegradeTree * index) noexcept : _index(index) {}

        ~BitDegradeTreeIterator() noexcept override = default;

        bool valid() const override {
            return _valid;
        }

        void seekToFirst() override {
            loadToLeftest();
        }

        void seekToLast() override {
            loadToRightest();
        }

        void seek(const Slice & k) override {
            loadToKey(k);
        }

        void seekForPrev(const Slice & k) override { seek(k); }

        void next() override {
            assert(valid());
            if ((++_cursor) >= _node_clone.immut_ptrs().size()
                || _node_clone.immut_ptrs()[_cursor].isNull()
                || _node_clone.immut_ptrs()[_cursor].isNode()) {
                reloadToRight();
            } else {
                configureUsrForNext();
            }
        }

        void prev() override {
            assert(valid());
            if ((--_cursor) < 0
                || _node_clone.immut_ptrs()[_cursor].isNull()
                || _node_clone.immut_ptrs()[_cursor].isNode()) {
                reloadToLeft();
            } else {
                configureUsrForPrev();
            }
        }

        Slice key() const override {
            assert(valid());
            return _usr.toSlice();
        }

        OffsetToData value() const override {
            assert(valid());
            return _node_clone.immut_ptrs()[_cursor].asData();
        }

    private:
        void loadToLeftest() { loadToTarget<false>({}); }

        void loadToRightest() { loadToTarget<true>({}); }

        void loadToKey(const Slice & k) {
            _usr.clear();
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
                    return;
                }
            }
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
            // @formatter:off
#ifndef NDEBUG
            auto pos =
#endif
                    findBestMatch<false>(&_node_clone, largerKey(), &_usr);
#ifndef NDEBUG
            assert(pos.first + pos.second == _cursor);
#endif
            // @formatter:on
        }

        void configureUsrForPrev() noexcept {
            // @formatter:off
#ifndef NDEBUG
            auto pos =
#endif
                    findBestMatch<true>(&_node_clone, smallerKey(), &_usr);
#ifndef NDEBUG
            assert(pos.first + pos.second == _cursor);
#endif
            // @formatter:on
        }

    private:
        template<bool RIGHT_FIRST>
        std::pair<size_t, bool>
        findBestMatch(const BDNode * node, const Slice & target, USR * reveal_info) noexcept {
            const uint32_t * cbegin = node->immut_diffs().cbegin();
            const uint32_t * cend;

            size_t size = node->size();
            if (size <= 1) {
                reveal_info->mut_src().resize(1);
                reveal_info->mut_src().front() = 0;
                reveal_info->mut_extra().resize(1);
                reveal_info->mut_extra().front() = 0;
                return {0, false};
            }
            cend = &node->immut_diffs()[size - 1];

            auto cmp = [node](const uint32_t & a, const uint32_t & b) noexcept {
                return a < b || (a == b && (node->immut_masks()[&a - node->immut_diffs().cbegin()] & ~(1 << 7)) >
                                           (node->immut_masks()[&b - node->immut_diffs().cbegin()] & ~(1 << 7)));
            };

            while (true) {
                const uint32_t * min_it = std::min_element(cbegin, cend, cmp);
                uint32_t diff_at = *min_it;
                uint8_t trans_mask = transMask(node->immut_masks()[min_it - node->immut_diffs().cbegin()]);

                uint8_t crit_byte = target.size() > diff_at ? charToUint8(target[diff_at])
                                                            : static_cast<uint8_t>(RIGHT_FIRST ? UINT8_MAX : 0);
                auto direct = static_cast<bool>((1 + (crit_byte | trans_mask)) >> 8);
                reveal_info->reveal(diff_at, uint8ToChar(trans_mask), direct);

                if (!direct) { // left
                    cend = min_it;
                } else { // right
                    cbegin = min_it + 1;
                }

                if (cbegin == cend) {
                    return {min_it - node->immut_diffs().cbegin(), direct};
                }
            }
        };

        template<bool RIGHT_FIRST>
        void loadToTarget(const Slice & target) {
            _usr.clear();
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
                    return;
                }
            }
        }

        Slice smallerKey() const noexcept {
            Slice s = key();
            Slice res;

            size_t i = s.size();
            do {
                --i;
                if ((_usr.immut_extra()[i] & _usr.immut_src()[i]) == 0) {
                    assert(i != 0);
                    continue;
                }

                auto * arr = reinterpret_cast<char *>(malloc(i + 1));
                res = Slice::pinnableSlice(arr, i + 1);
                memcpy(arr, s.data(), res.size());

                --arr[i];
                // coverity[leaked_storage]
                break;
            } while (true);

            assert(res.owned());
            return res;
        }

        Slice largerKey() const noexcept {
            Slice s = key();
            Slice res;

            size_t i = s.size();
            do {
                --i;
                char xor_res;
                if ((xor_res = (_usr.immut_extra()[i] ^ _usr.immut_src()[i])) == 0) {
                    assert(i != 0);
                    continue;
                }

                auto * arr = reinterpret_cast<char *>(malloc(i + 1));
                res = Slice::pinnableSlice(arr, i + 1);
                memcpy(arr, s.data(), res.size());

                auto n = __builtin_ffs(xor_res);
                arr[i] |= 1 << (n - 1); // 0 to 1
                arr[i] &= uint8ToChar(UINT8_MAX >> (n - 1) << (n - 1));
                // coverity[leaked_storage]
                break;
            } while (true);

            assert(res.owned());
            return res;
        }

        bool metMin() const noexcept {
            return std::all_of(_usr.immut_src().cbegin(), _usr.immut_src().cend(), [](char a) noexcept {
                return a == 0;
            });
        }

        bool metMax() const noexcept {
            return std::all_of(_usr.immut_src().cbegin(), _usr.immut_src().cend(), [this](const char & a) noexcept {
                return (a ^ _usr.immut_extra()[&a - &_usr.immut_src().front()]) == 0;
            });
        }
    };

    std::unique_ptr<Iterator<Slice/* usr */, OffsetToData>>
    BitDegradeTree::scan() const noexcept {
        return std::make_unique<BitDegradeTreeIterator>(this);
    }
}