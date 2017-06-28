#ifndef LEVIDB_ARENA_H
#define LEVIDB_ARENA_H

#include <cstddef>
#include <vector>

namespace LeviDB {
    class Arena {
    public:
        Arena() noexcept: _alloc_ptr(nullptr), _alloc_bytes_remaining(0) {};

        ~Arena() noexcept {};

        char * allocate(size_t bytes) noexcept;

        char * allocateAligned(size_t bytes) noexcept;

    private:
        char * _alloc_ptr;
        size_t _alloc_bytes_remaining;
        std::vector<std::unique_ptr<char[]>> _blocks;

        char * allocateFallback(size_t bytes) noexcept;

        char * allocateNewBlock(size_t block_bytes) noexcept;
    };
}

#endif //LEVIDB_ARENA_H