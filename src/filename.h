#pragma once
#ifndef LEVIDB_FILENAME_H
#define LEVIDB_FILENAME_H

/*
 * Index 命名规则 index_ + [0, 1, 2, 3, ...]
 * Store 命名规则 store_ + seq + _ + lv + [.cprs, .plain]
 */

#include <string>

namespace levidb {
    bool IsCompressedStore(const std::string & fname);

    bool IsPlainStore(const std::string & fname);

    bool IsIndex(const std::string & fname);

    bool IsStore(const std::string & fname);

    size_t GetStoreSeq(const std::string & fname);

    size_t GetStoreLv(const std::string & fname);

    void IndexFilename(size_t nth, const std::string & dirname,
                       std::string * fname);

    void StoreFilename(size_t seq, size_t lv, bool compress, const std::string & dirname,
                       std::string * fname);
}

#endif //LEVIDB_FILENAME_H
