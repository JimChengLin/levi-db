#pragma once
#ifndef LEVIDB8_OPTIONS_H
#define LEVIDB8_OPTIONS_H

/*
 * 运行时参数
 */

namespace levidb8 {
    struct PutOptions {
        bool sync = false;
    };

    struct RemoveOptions {
        bool sync = false;
    };

    struct WriteOptions {
        bool sync = false;
        bool try_compress = true;
    };

    struct ReadOptions {

    };

    struct ScanOptions {

    };

    struct OpenOptions {
        bool create_if_missing = false;
        bool error_if_exists = false;
    };
}

#endif //LEVIDB8_OPTIONS_H
