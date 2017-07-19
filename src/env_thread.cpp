#include "env_thread.h"
#include <algorithm>
#include <pthread.h>

#ifndef __clang__
#include <cstring>
#endif

namespace LeviDB {
    namespace ThreadEnv {
        uint64_t gettid() noexcept {
            pthread_t tid = pthread_self();
            uint64_t thread_id = 0;
            memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
            return thread_id;
        }
    }
}