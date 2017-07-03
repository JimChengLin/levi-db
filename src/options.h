#ifndef LEVIDB_OPTIONS_H
#define LEVIDB_OPTIONS_H

/*
 * 运行参数与编译常数
 */

#include "env.h"

namespace LeviDB {
    namespace OptionsConst {
    }

    struct Options {
        bool create_if_missing;
        bool error_if_exists;
        bool compress;
        Logger * info_log;

        Options() noexcept
                : create_if_missing(false),
                  error_if_exists(false),
                  compress(true),
                  info_log(nullptr) {};
    };

    struct ReadOptions {
    };

    struct WriteOptions {
        bool sync;

        WriteOptions() noexcept : sync(false) {}
    };
}

#endif //LEVIDB_OPTIONS_H