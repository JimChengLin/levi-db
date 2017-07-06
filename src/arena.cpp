#include "arena.h"

#include <cassert>

namespace LeviDB {
    static constexpr int block_size = 4096;

    char * Arena::allocate(size_t bytes) noexcept {
        assert(bytes > 0);
        if (bytes <= _alloc_bytes_remaining) {
            char * res = _alloc_ptr;
            _alloc_ptr += bytes;
            _alloc_bytes_remaining -= bytes;
            return res;
        }
        return allocateFallback(bytes);
    }

    char * Arena::allocateFallback(size_t bytes) noexcept {
        if (bytes > block_size / 4) {
            return allocateNewBlock(bytes);
        }

        _alloc_ptr = allocateNewBlock(block_size);
        _alloc_bytes_remaining = block_size;

        char * res = _alloc_ptr;
        _alloc_ptr += bytes;
        _alloc_bytes_remaining -= bytes;
        return res;
    }

    char * Arena::allocateAligned(size_t bytes) noexcept {
        static constexpr int align = sizeof(void *);
        static_assert((align & (align - 1)) == 0, "align should be 2^x");

        size_t current_mod = reinterpret_cast<uintptr_t>(_alloc_ptr) & (align - 1);
        size_t slop = (current_mod == 0 ? 0 : align - current_mod);
        size_t needed = bytes + slop;

        char * res;
        if (needed <= _alloc_bytes_remaining) {
            res = _alloc_ptr + slop;
            _alloc_ptr += needed;
            _alloc_bytes_remaining -= needed;
        } else {
            res = allocateFallback(bytes);
        }
        assert((reinterpret_cast<uintptr_t>(res) & (align - 1)) == 0);

        return res;
    }

    char * Arena::allocateNewBlock(size_t block_bytes) noexcept {
        auto smart_ptr = std::unique_ptr<char[]>(new char[block_bytes]);
        char * res = smart_ptr.get();
        _blocks.emplace_back(move(smart_ptr));
        return res;
    }

    void Arena::reset() noexcept {
        _alloc_ptr = nullptr;
        _alloc_bytes_remaining = 0;
        _blocks.clear();
    };
}