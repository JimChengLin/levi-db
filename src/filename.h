#pragma once
#ifndef LEVIDB_FILENAME_H
#define LEVIDB_FILENAME_H

/*
 * Index 命名规则 index_ + [0, 1, 2, 3, ...]
 * Store 命名规则 store_ + seq + _ + lv + [.cprs, .plain]
 */

#include <string>

namespace levidb {
    constexpr char kManifestFilename[] = "MANIFEST";

    bool IsCompressedStore(const std::string & fname);

    bool IsPlainStore(const std::string & fname);

    bool IsIndex(const std::string & fname);

    bool IsStore(const std::string & fname);

    uint32_t GetStoreSeq(const std::string & fname);

    uint32_t GetStoreLv(const std::string & fname);

    void IndexFilename(uint32_t nth, std::string * fname);

    void StoreFilename(uint32_t seq, uint32_t lv, std::string * fname);
}

#endif //LEVIDB_FILENAME_H
