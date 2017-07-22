#ifndef LEVIDB_ARENA_H
#define LEVIDB_ARENA_H

/*
 * 内存池
 */

#include <cstddef>
#include <memory>
#include <vector>

namespace LeviDB {
    class Arena {
    public:
        Arena() noexcept = default;

        ~Arena() noexcept = default;

        char * allocate(size_t bytes) noexcept;

        char * allocateAligned(size_t bytes) noexcept;

        void reset() noexcept;

    private:
        std::vector<std::unique_ptr<char[]>> _blocks;
        size_t _alloc_bytes_remaining = 0;
        char * _alloc_ptr = nullptr;

        char * allocateFallback(size_t bytes) noexcept;

        char * allocateNewBlock(size_t block_bytes) noexcept;
    };
}

#endif //LEVIDB_ARENA_H