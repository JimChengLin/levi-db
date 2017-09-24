#include <algorithm>

#ifndef __clang__
#include <cstring>
#endif

#include "env_thread.h"
#include "exception.h"

namespace LeviDB {
    namespace ThreadEnv {
        uint64_t gettid() noexcept {
            pthread_t tid = pthread_self();
            uint64_t thread_id = 0;
            memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
            return thread_id;
        };
    }

    RWLockReadGuard::RWLockReadGuard(pthread_rwlock_t * lock) : _lock(lock) {
        if (pthread_rwlock_rdlock(_lock) != 0) {
            throw Exception::corruptionException("rdlock fail");
        }
    }

    RWLockReadGuard::~RWLockReadGuard() noexcept {
        if (_lock != nullptr) pthread_rwlock_unlock(_lock);
    }

    RWLockWriteGuard::RWLockWriteGuard(pthread_rwlock_t * lock) : _lock(lock) {
        if (pthread_rwlock_wrlock(_lock) != 0) {
            throw Exception::corruptionException("wrlock fail");
        }
    }

    RWLockWriteGuard::~RWLockWriteGuard() noexcept {
        if (_lock != nullptr) pthread_rwlock_unlock(_lock);
    }
}