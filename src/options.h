#ifndef LEVIDB_OPTIONS_H
#define LEVIDB_OPTIONS_H

/*
 * 运行时参数
 */

#include "seq_gen.h"

namespace LeviDB {
    struct Options {
        bool create_if_missing = false;
        bool error_if_exists = false;
        bool compression = true;
    };

    struct ReadOptions {
        uint64_t sequence_number = 0;
    };

    struct IteratorOptions {
        std::unique_ptr<Snapshot> snapshot;
    };

    struct WriteOptions {
        bool sync = false;
    };
}

#endif //LEVIDB_OPTIONS_H