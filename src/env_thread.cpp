#include <algorithm>
#include <cassert>

#ifndef __clang__

#include <cstring>

#endif

#include "env_thread.h"
#include "util.h"

#define ASSERT_WL_HOLD(lock) assert((lock)->_need_write)
#define ASSERT_RL_HOLD(lock) assert((lock)->_cnt > 0)

namespace levidb8 {
    namespace env_thread {
        uint64_t gettid() noexcept {
            const pthread_t tid = pthread_self();
            uint64_t thread_id = 0;
            memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
            return thread_id;
        };
    }

    RWLockReadGuard::RWLockReadGuard(ReadWriteLock * lock) noexcept : _lock(lock) {
        while (true) {
            // 等待当前独占锁(写锁)
            if (UNLIKELY(lock->_need_write.load(std::memory_order_acquire))) {
                std::this_thread::yield();
                continue;
            }
            // 写锁释放, 读锁计数加一
            lock->_cnt.fetch_add(1);
            // 写锁释放的一瞬间有可能再次被占用, 二次检测确认
            if (UNLIKELY(lock->_need_write.load(std::memory_order_acquire))) {
                // 读锁获取失败, 计数器减一还原, 继续等待
                lock->_cnt.fetch_sub(1);
                continue;
            }
            break;
        }
        ASSERT_RL_HOLD(lock);
    }

    RWLockReadGuard::RWLockReadGuard(RWLockReadGuard && another) noexcept : _lock(another._lock) {
        another._lock = nullptr;
    }

    RWLockReadGuard & RWLockReadGuard::operator=(RWLockReadGuard && another) noexcept {
        std::swap(_lock, another._lock);
        return *this;
    }

    RWLockReadGuard::~RWLockReadGuard() noexcept {
        if (_lock != nullptr) {
            ASSERT_RL_HOLD(_lock);
            _lock->_cnt.fetch_sub(1);
        }
    }

    void RWLockReadGuard::release() noexcept {
        ASSERT_RL_HOLD(_lock);
        _lock->_cnt.fetch_sub(1);
        _lock = nullptr;
    }

    bool RWLockReadGuard::tryUpgrade(RWLockReadGuard * read_guard, RWLockWriteGuard * write_guard) noexcept {
        ReadWriteLock * lock = read_guard->_lock;
        bool e = false;
        if (lock->_need_write.compare_exchange_strong(e, true)) {
            write_guard->_lock = lock;
            read_guard->release();

            if (lock->_cnt.load(std::memory_order_acquire) != 0) {
                asm volatile("pause");
                while (lock->_cnt.load(std::memory_order_acquire) != 0) {
                    std::this_thread::yield();
                }
            }
            ASSERT_WL_HOLD(lock);
            return true;
        }
        return false;
    }

    RWLockWriteGuard::RWLockWriteGuard(ReadWriteLock * lock) noexcept : _lock(lock) {
        while (true) {
            if (UNLIKELY(lock->_need_write.load(std::memory_order_acquire))) {
                std::this_thread::yield();
                continue;
            }
            // 有可能多个线程竞争写锁, 必须用 CAS, 败者继续等待
            bool e = false;
            if (lock->_need_write.compare_exchange_strong(e, true)) {
                break;
            }
        }
        // 写锁已经获取到了, 但读锁计数必须归零才能继续
        if (LIKELY(lock->_cnt.load(std::memory_order_acquire) != 0)) {
            asm volatile("pause");

            while (true) {
                if (LIKELY(lock->_cnt.load(std::memory_order_acquire) != 0)) {
                    std::this_thread::yield();
                    continue;
                }
                break;
            }
        }
        ASSERT_WL_HOLD(lock);
    }

    RWLockWriteGuard::RWLockWriteGuard(RWLockWriteGuard && another) noexcept : _lock(another._lock) {
        another._lock = nullptr;
    }

    RWLockWriteGuard & RWLockWriteGuard::operator=(RWLockWriteGuard && another) noexcept {
        std::swap(_lock, another._lock);
        return *this;
    }

    RWLockWriteGuard::~RWLockWriteGuard() noexcept {
        if (_lock != nullptr) {
            ASSERT_WL_HOLD(_lock);
            // 不存在竞争
            _lock->_need_write.store(false, std::memory_order_release);
        }
    }

    void RWLockWriteGuard::release() noexcept {
        _lock->_need_write.store(false, std::memory_order_release);
        _lock = nullptr;
    }

    void RWLockWriteGuard::degrade(RWLockWriteGuard * write_guard, RWLockReadGuard * read_guard) noexcept {
        write_guard->_lock->_cnt.fetch_add(1);
        read_guard->_lock = write_guard->_lock;
        write_guard->release();
        ASSERT_RL_HOLD(read_guard->_lock);
    }
}