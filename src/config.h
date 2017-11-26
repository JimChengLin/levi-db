#pragma once
#ifndef LEVIDB8_CONFIG_H
#define LEVIDB8_CONFIG_H

/*
 * 配置
 */

#include <cstddef>
#include <cstdint>

namespace levidb8 {
    static constexpr int kPageSize = 4096;
    static constexpr int kRank = 454;
    static constexpr uint32_t kDiskNull = UINT32_MAX;
    static constexpr uint32_t kFileAddressLimit = UINT32_MAX;

    static constexpr size_t kLogBlockSize = 32768; // 2^15
    static constexpr size_t kLogHeaderSize = 4/* checksum */+ 1/* type */+ 2/* length */;

    static constexpr int kZstdLevel = 1;
}

#endif //LEVIDB8_CONFIG_H
