#ifndef LEVIDB_ARENA_H
#define LEVIDB_ARENA_H

/*
 * 内存池
 */

#include <cstddef>
#include <vector>

namespace LeviDB {
    class Arena {
    public:
        Arena() noexcept: _alloc_ptr(nullptr), _alloc_bytes_remaining(0) {};

        ~Arena() noexcept {};

        char * allocate(size_t bytes) noexcept;

        char * allocateAligned(size_t bytes) noexcept;

        void reset() noexcept;

    private:
        std::vector<std::unique_ptr<char[]>> _blocks;
        size_t _alloc_bytes_remaining;
        char * _alloc_ptr;

        char * allocateFallback(size_t bytes) noexcept;

        char * allocateNewBlock(size_t block_bytes) noexcept;
    };
}

#endif //LEVIDB_ARENA_H