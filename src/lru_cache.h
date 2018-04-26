#pragma once
#ifndef LEVIDB_LRU_CACHE_H
#define LEVIDB_LRU_CACHE_H

/*
 * Original File:   lrucache.hpp
 * Original Author: Alexander Ponomarev
 */

#include <list>
#include <unordered_map>

namespace levidb {
    template<typename K, typename V, size_t MAX>
    class LRUCache {
    private:
        typedef typename std::pair<K, V> key_value_pair_t;
        typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

        mutable std::list<key_value_pair_t> cache_items_list_;
        mutable std::unordered_map<K, list_iterator_t> cache_items_map_;

    public:
        LRUCache() = default;

        LRUCache(const LRUCache &) = delete;

        LRUCache & operator=(const LRUCache &) = delete;

    public:
        template<typename T>
        void Add(const K & k, T && v) {
            auto it = cache_items_map_.find(k);
            if (it == cache_items_map_.end()) {
                if (size() >= MAX) {
                    auto last = --cache_items_list_.end();
#if defined(__cpp_lib_node_extract)
                    auto nh = cache_items_map_.extract(last->first);
                    nh.key() = k;
                    cache_items_map_.insert(std::move(nh));

                    last->first = k;
                    last->second = std::forward<T>(v);
                    cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, last);
                    return;
#else
                    cache_items_map_.erase(last->first);
                    cache_items_list_.pop_back();
#endif
                }
                cache_items_list_.emplace_front(k, std::forward<T>(v));
                cache_items_map_.emplace(k, cache_items_list_.begin());
            } else {
                it->second->second = std::forward<T>(v);
                cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
            }
        }

        bool Get(const K & k, V * v) const {
            auto it = cache_items_map_.find(k);
            if (it == cache_items_map_.cend()) {
                return false;
            } else {
                cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
                *v = it->second->second;
                return true;
            }
        }

        bool Exists(const K & k) const {
            return cache_items_map_.find(k) != cache_items_map_.cend();
        }

        size_t size() const {
            return cache_items_map_.size();
        }
    };
}

#endif //LEVIDB_LRU_CACHE_H
