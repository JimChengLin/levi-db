#ifndef LEVIDB_ENV_THREAD_H
#define LEVIDB_ENV_THREAD_H

/*
 * 线程 API 封装
 */

#include <cstdint>

namespace LeviDB {
    namespace ThreadEnv {
        uint64_t gettid() noexcept;
    }
}

#endif //LEVIDB_ENV_THREAD_H