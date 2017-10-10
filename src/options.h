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

        Options createIfMissing(bool val) const noexcept {
            Options res = *this;
            res.create_if_missing = val;
            return res;
        }

        Options errorIfExists(bool val) const noexcept {
            Options res = *this;
            res.error_if_exists = val;
            return res;
        }
    };

    struct ReadOptions {
        uint64_t sequence_number = 0;
    };

    struct WriteOptions {
        uint32_t uncompress_size = 0; // if you are using LvDB, then you can ignore this one completely
        bool sync = false;
        bool compress = false;
    };
}

#endif //LEVIDB_OPTIONS_H