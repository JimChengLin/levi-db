#ifndef LEVIDB_OPTIONS_H
#define LEVIDB_OPTIONS_H

/*
 * 运行参数与编译常数
 */

namespace LeviDB {
    namespace OptionsConst {
    }

    struct Options {
        bool create_if_missing = false;
        bool error_if_exists = false;
        bool compress = true;
    };

    struct ReadOptions {
    };

    struct WriteOptions {
        bool sync = false;
    };
}

#endif //LEVIDB_OPTIONS_H