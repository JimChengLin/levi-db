#ifndef LEVIDB_ENV_THREAD_H
#define LEVIDB_ENV_THREAD_H

/*
 * 线程 API 封装
 */

#include <cstdint>
#include <pthread.h>
#include <thread>
#include <type_traits>

#include "util.h"

namespace LeviDB {
    static_assert(std::is_same<std::thread::native_handle_type, pthread_t>::value,
                  "cannot mix std::thread with pthread");

    namespace ThreadEnv {
        uint64_t gettid() noexcept;
    }

    class ReadWriteLock {
    private:
        pthread_rwlock_t _rwlock = PTHREAD_RWLOCK_INITIALIZER;

        friend class RWLockReadGuard;

        friend class RWLockWriteGuard;

    public:
        ReadWriteLock() noexcept = default;

        DELETE_MOVE(ReadWriteLock);
        DELETE_COPY(ReadWriteLock);

        ~ReadWriteLock() noexcept { pthread_rwlock_destroy(&_rwlock); }
    };

    class RWLockReadGuard {
    private:
        pthread_rwlock_t * _lock = nullptr;

    public:
        RWLockReadGuard() noexcept = default;

        explicit RWLockReadGuard(pthread_rwlock_t * lock);

        explicit RWLockReadGuard(const ReadWriteLock & lock)
                : RWLockReadGuard(&const_cast<ReadWriteLock &>(lock)._rwlock) {};

        RWLockReadGuard(RWLockReadGuard && another) noexcept { operator=(std::move(another)); };

        RWLockReadGuard & operator=(RWLockReadGuard && another) noexcept;

        DELETE_COPY(RWLockReadGuard);

        ~RWLockReadGuard() noexcept;
    };

    class RWLockWriteGuard {
    private:
        pthread_rwlock_t * _lock = nullptr;

    public:
        RWLockWriteGuard() noexcept = default;

        explicit RWLockWriteGuard(pthread_rwlock_t * lock);

        explicit RWLockWriteGuard(ReadWriteLock & lock)
                : RWLockWriteGuard(&lock._rwlock) {};

        RWLockWriteGuard(RWLockWriteGuard && another) noexcept { operator=(std::move(another)); };

        RWLockWriteGuard & operator=(RWLockWriteGuard && another) noexcept;

        DELETE_COPY(RWLockWriteGuard);

        ~RWLockWriteGuard() noexcept;
    };
}

#endif //LEVIDB_ENV_THREAD_H