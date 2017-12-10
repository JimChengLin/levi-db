#include "env_thread.h"

namespace levidb8 {
    RWLockReadGuard::RWLockReadGuard(ReadWriteLock * lock) noexcept : _lock(lock) {
        while (true) {
            if (lock->_need_write.load(std::memory_order_acquire)) {
                std::this_thread::yield();
                continue;
            }
            lock->_cnt.fetch_add(1);
            if (lock->_need_write.load(std::memory_order_acquire)) {
                lock->_cnt.fetch_sub(1);
                continue;
            }
            break;
        }
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
            _lock->_cnt.fetch_sub(1);
        }
    }

    void RWLockReadGuard::release() noexcept {
        _lock->_cnt.fetch_sub(1);
        _lock = nullptr;
    }

    bool RWLockReadGuard::tryUpgrade(RWLockReadGuard * read_guard, RWLockWriteGuard * write_guard) noexcept {
        ReadWriteLock * lock = read_guard->_lock;
        if (!lock->_need_write.exchange(true, std::memory_order_acquire)) {
            write_guard->_lock = lock;
            read_guard->release();

            if (lock->_cnt.load(std::memory_order_acquire) != 0) {
                asm volatile("pause");
                while (lock->_cnt.load(std::memory_order_acquire) != 0) {
                    std::this_thread::yield();
                }
            }
            return true;
        }
        return false;
    }

    RWLockWriteGuard::RWLockWriteGuard(ReadWriteLock * lock) noexcept : _lock(lock) {
        while (true) {
            if (lock->_need_write.load(std::memory_order_acquire)) {
                std::this_thread::yield();
                continue;
            }
            if (!lock->_need_write.exchange(true, std::memory_order_acquire)) {
                break;
            }
        }
        if (lock->_cnt.load(std::memory_order_acquire) != 0) {
            asm volatile("pause");
            while (lock->_cnt.load(std::memory_order_acquire) != 0) {
                std::this_thread::yield();
            }
        }
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
            _lock->_need_write.store(false, std::memory_order_release);
        }
    }

    void RWLockWriteGuard::release() noexcept {
        _lock->_need_write.store(false, std::memory_order_release);
        _lock = nullptr;
    }

    void RWLockWriteGuard::degrade(RWLockWriteGuard * write_guard, RWLockReadGuard * read_guard) noexcept {
        read_guard->_lock = write_guard->_lock;
        read_guard->_lock->_cnt.fetch_add(1);
        write_guard->release();
    }
}