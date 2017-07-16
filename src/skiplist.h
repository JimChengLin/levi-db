#ifndef LEVIDB_SKIPLIST_H
#define LEVIDB_SKIPLIST_H

/*
 * 内存使用 arena 分配的跳表
 */

#include "arena.h"
#include "random.h"
#include <cassert>
#include <functional>
#include <vector>

namespace LeviDB {
    struct less {
        template<typename T>
        int operator()(T && a, T && b) const {
            if (a < b) {
                return -1;
            } else if (a == b) {
                return 0;
            } else {
                return 1;
            }
        }
    };

    template<typename K, class CMP=less>
    class SkipList {
    private:
        struct Node;

    public:
        explicit SkipList(Arena * arena) noexcept;

        explicit SkipList(SkipList && another) noexcept;

        SkipList(Arena * arena, CMP cmp) noexcept;

        ~SkipList() noexcept {}

        const K * insert(const K & key) noexcept;

        const K * insert(K && key) noexcept;

        bool contains(const K & key) const noexcept;

        const K * find(const K & key) const noexcept;

        const K * findOrGreater(const K & key) const noexcept;

        bool empty() const noexcept;

        void move_to_fit(const K & old_key, const K & new_key) noexcept;

        void move_to_fit(const K & old_key, K && new_key) noexcept;

        class Iterator {
        public:
            explicit Iterator(const SkipList * list) noexcept
                    : _list(list), _node(nullptr) {};

            inline bool valid() const noexcept { return _node != nullptr; };

            inline const K & key() const noexcept {
                assert(valid());
                return _node->key;
            };

            inline void next() noexcept {
                assert(valid());
                _node = _node->next(0);
            };

            inline void prev() noexcept {
                assert(valid());
                _node = _list->findLessThan(_node->key);
                if (_node == _list->_head) {
                    _node = nullptr;
                }
            };

            inline void seek(const K & target) noexcept {
                _node = _list->findGreaterOrEqual(target, nullptr);
            };

            inline void seekToFirst() noexcept {
                _node = _list->_head->next(0);
            };

            inline void seekToLast() noexcept {
                _node = _list->findLast();
                if (_node == _list->_head) {
                    _node = nullptr;
                }
            };

        private:
            const SkipList * _list;
            const Node * _node;
        };

    public:
        const CMP _comparator;

    private:
        static constexpr int max_height = 12;

        Arena * const _arena;
        Node * const _head;

        Random _rnd;
        int _this_max_h;

        auto newNode(const K & key, int height) noexcept;

        auto newNode(K && key, int height) noexcept;

        auto newNode(int height) noexcept;

        int randomHeight() noexcept;

        bool equal(const K & a, const K & b) const noexcept { return _comparator(a, b) == 0; };

        bool keyIsAfterNode(const K & key, const Node * n) const noexcept;

        auto findGreaterOrEqual(const K & key, Node * prev[]) const noexcept;

        auto findLessThan(const K & key) const noexcept;

        auto findLast() const noexcept;

        // 禁止复制
        SkipList(const SkipList &);

        void operator=(const SkipList &);
    };

    template<typename K, class CMP>
    struct SkipList<K, CMP>::Node {
        K key;
        Node * next_arr[1];

        explicit Node(const K & k) noexcept : key(k) {};

        explicit Node(K && k) noexcept : key(std::move(k)) {};

        Node * next(int n) const noexcept { return next_arr[n]; }

        void setNext(int n, Node * ptr) noexcept { next_arr[n] = ptr; }
    };

    template<typename K, class CMP>
    auto SkipList<K, CMP>::newNode(const K & key, int height) noexcept {
        char * mem = _arena->allocateAligned(sizeof(Node) + sizeof(Node *) * (height - 1));
        return new(mem) Node(key);
    }

    template<typename K, class CMP>
    auto SkipList<K, CMP>::newNode(K && key, int height) noexcept {
        char * mem = _arena->allocateAligned(sizeof(Node) + sizeof(Node *) * (height - 1));
        return new(mem) Node(std::move(key));
    }

    template<typename K, class CMP>
    auto SkipList<K, CMP>::newNode(int height) noexcept { // only for _head
        char * mem = _arena->allocateAligned(sizeof(Node) + sizeof(Node *) * (height - 1));
        return reinterpret_cast<Node *>(mem);
    }

    template<typename K, class CMP>
    int SkipList<K, CMP>::randomHeight() noexcept {
        static constexpr int branch_factor = 4;
        int height = 1;
        while (height < max_height && _rnd.oneIn(branch_factor)) {
            ++height;
        }
        assert(height > 0);
        assert(height <= max_height);
        return height;
    }

    template<typename K, class CMP>
    bool SkipList<K, CMP>::keyIsAfterNode(const K & key, const Node * n) const noexcept {
        return n != nullptr && _comparator(n->key, key) < 0;
    }

    template<typename K, class CMP>
    auto SkipList<K, CMP>::findGreaterOrEqual(const K & key, Node * prev[]) const noexcept {
        Node * x = _head;
        int curr_lv = _this_max_h - 1;
        while (true) {
            Node * next = x->next(curr_lv);
            if (keyIsAfterNode(key, next)) {
                x = next;
            } else {
                if (prev != nullptr) {
                    prev[curr_lv] = x;
                }
                if (curr_lv == 0) {
                    return next;
                } else {
                    --curr_lv;
                }
            }
        }
    }

    template<typename K, class CMP>
    auto SkipList<K, CMP>::findLessThan(const K & key) const noexcept {
        Node * x = _head;
        int curr_lv = _this_max_h - 1;
        while (true) {
            Node * next = x->next(curr_lv);
            if (next == nullptr || _comparator(next->key, key) >= 0) {
                if (curr_lv == 0) {
                    return x;
                } else {
                    --curr_lv;
                }
            } else {
                x = next;
            }
        }
    }

    template<typename K, class CMP>
    auto SkipList<K, CMP>::findLast() const noexcept {
        Node * x = _head;
        int curr_lv = _this_max_h - 1;
        while (true) {
            Node * next = x->next(curr_lv);
            if (next == nullptr) {
                if (curr_lv == 0) {
                    return x;
                } else {
                    --curr_lv;
                }
            } else {
                x = next;
            }
        }
    }

    template<typename K, class CMP>
    SkipList<K, CMP>::SkipList(Arena * arena) noexcept
            :_comparator(),
             _arena(arena),
             _head(newNode(max_height)),
             _rnd(0xdeadbeef),
             _this_max_h(1) {
        for (int i = 0; i < max_height; ++i) {
            _head->setNext(i, nullptr);
        }
    }

    template<typename K, class CMP>
    SkipList<K, CMP>::SkipList(SkipList && another) noexcept
            :_comparator(another._comparator),
             _arena(another._arena),
             _head(another._head),
             _this_max_h(another._this_max_h),
             _rnd(another._rnd) {
        another._head->setNext(0, nullptr);
    }

    template<typename K, class CMP>
    SkipList<K, CMP>::SkipList(Arena * arena, CMP cmp) noexcept
            :_comparator(cmp),
             _arena(arena),
             _head(newNode(max_height)),
             _rnd(0xdeadbeef),
             _this_max_h(1) {
        for (int i = 0; i < max_height; ++i) {
            _head->setNext(i, nullptr);
        }
    }

    template<typename K, class CMP>
    const K * SkipList<K, CMP>::insert(const K & key) noexcept {
        Node * prev[max_height];
        Node * x = findGreaterOrEqual(key, prev);
        if (x != nullptr && equal(key, x->key)) {
            x->key = key;
            return &x->key;
        }

        int height = randomHeight();
        if (height > _this_max_h) {
            for (int i = _this_max_h; i < height; ++i) {
                prev[i] = _head;
            }
            _this_max_h = height;
        }

        x = newNode(key, height);
        for (int i = 0; i < height; ++i) {
            x->setNext(i, prev[i]->next(i));
            prev[i]->setNext(i, x);
        }
        return &x->key;
    }

    template<typename K, class CMP>
    const K * SkipList<K, CMP>::insert(K && key) noexcept {
        Node * prev[max_height];
        Node * x = findGreaterOrEqual(key, prev);
        if (x != nullptr && equal(key, x->key)) {
            x->key = std::move(key);
            return &x->key;
        }

        int height = randomHeight();
        if (height > _this_max_h) {
            for (int i = _this_max_h; i < height; ++i) {
                prev[i] = _head;
            }
            _this_max_h = height;
        }

        x = newNode(std::move(key), height);
        for (int i = 0; i < height; ++i) {
            x->setNext(i, prev[i]->next(i));
            prev[i]->setNext(i, x);
        }
        return &x->key;
    }

    template<typename K, class CMP>
    bool SkipList<K, CMP>::contains(const K & key) const noexcept {
        Node * x = findGreaterOrEqual(key, nullptr);
        return x != nullptr && equal(key, x->key);
    }

    template<typename K, class CMP>
    const K * SkipList<K, CMP>::find(const K & key) const noexcept {
        Node * x = findGreaterOrEqual(key, nullptr);
        return x != nullptr && equal(key, x->key) ? &x->key : nullptr;
    }

    template<typename K, class CMP>
    const K * SkipList<K, CMP>::findOrGreater(const K & key) const noexcept {
        Node * x = findGreaterOrEqual(key, nullptr);
        return x != nullptr ? &x->key : nullptr;
    }

    template<typename K, class CMP>
    bool SkipList<K, CMP>::empty() const noexcept {
        return _head->next(0) == nullptr;
    }

    template<typename K, class CMP>
    void SkipList<K, CMP>::move_to_fit(const K & old_key, const K & new_key) noexcept {
        Node * prev[max_height];
        Node * x = findGreaterOrEqual(old_key, prev);
        assert(equal(old_key, x->key));

        int height = 0;
        for (; height < _this_max_h && prev[height]->next(height) == x; ++height) {
            prev[height]->setNext(height, x->next(height));
        }
        assert(height > 0);

        x->key = new_key;
        Node * y = findGreaterOrEqual(new_key, prev);
        assert(y == nullptr || !equal(new_key, y->key));

        for (int i = 0; i < height; ++i) {
            x->setNext(i, prev[i]->next(i));
            prev[i]->setNext(i, x);
        }
    }

    template<typename K, class CMP>
    void SkipList<K, CMP>::move_to_fit(const K & old_key, K && new_key) noexcept {
        Node * prev[max_height];
        Node * x = findGreaterOrEqual(old_key, prev);
        assert(equal(old_key, x->key));

        int height = 0;
        for (; height < _this_max_h && prev[height]->next(height) == x; ++height) {
            prev[height]->setNext(height, x->next(height));
        }
        assert(height > 0);

        x->key = std::move(new_key);
        Node * y = findGreaterOrEqual(x->key, prev);
        assert(y == nullptr || !equal(x->key, y->key));

        for (int i = 0; i < height; ++i) {
            x->setNext(i, prev[i]->next(i));
            prev[i]->setNext(i, x);
        }
    }
}

#endif //LEVIDB_SKIPLIST_H