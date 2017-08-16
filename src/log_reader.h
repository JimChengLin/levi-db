#ifndef LEVIDB_LOG_READER_H
#define LEVIDB_LOG_READER_H

#include <cstdint>
#include <memory>
#include <string>

#include "env_io.h"
#include "iterator.h"
#include "slice.h"

namespace LeviDB {
    class LogReader {
    public:
        using kv_iter = Iterator<Slice, std::string>; // because of CLion

        // 重复 seek 有优化
        // 结尾为 0 则不是 explicit del
        static std::unique_ptr<kv_iter> makeIterator(RandomAccessFile * data_file, uint32_t offset) noexcept;

        static
        std::unique_ptr<SimpleIterator<Slice>>
        makeRawIterator(RandomAccessFile * data_file, uint32_t offset) noexcept;
    };
}

#endif //LEVIDB_LOG_READER_H