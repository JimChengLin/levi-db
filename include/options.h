#pragma once
#ifndef LEVIDB_OPTIONS_H
#define LEVIDB_OPTIONS_H

/*
 * 运行时参数
 */

#include "manifestor.h"

namespace levidb {
    struct OpenOptions {
        Manifestor * manifestor = nullptr;
    };
}

#endif //LEVIDB_OPTIONS_H
