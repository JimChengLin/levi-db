#pragma once
#ifndef LEVIDB8_CONFIG_H
#define LEVIDB8_CONFIG_H

/*
 * 配置
 */

#include <cstddef>
#include <cstdint>

namespace levidb8 {
    static constexpr int kLenKeyLimit = UINT16_MAX >> 3;
    static constexpr int kPageSize = 4096;
    static constexpr int kRank = 682;
    static constexpr size_t kLogBlockSize = 32768; // 2^15
    static constexpr size_t kLogHeaderSize = 4/* checksum */+ 1/* type */+ 2/* length */;
    static constexpr uint32_t kDiskNull = UINT32_MAX;
    static constexpr uint32_t kIndexFileLimit = UINT32_MAX;
    static constexpr uint32_t kLogFileLimit = UINT32_MAX >> 1;

    static constexpr int kReaderDataCacheNum = 256;
    static constexpr int kReaderObjCacheNum = 16;
    static constexpr int kWorthCompressRatio = 8;
    static constexpr int kZstdLevel = 1;
}

#endif //LEVIDB8_CONFIG_H
