#pragma once
#ifndef LEVIDB_OPTIONS_H
#define LEVIDB_OPTIONS_H

/*
 * 运行时参数
 */

#include "compactor.h"
#include "manifestor.h"

namespace levidb {
    struct OpenOptions {
        Compactor * compactor = nullptr;
        Manifestor * manifestor = nullptr;
        bool create_if_missing = false;
        bool error_if_exist = false;
    };
}

#endif //LEVIDB_OPTIONS_H
