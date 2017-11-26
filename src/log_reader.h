#pragma once
#ifndef LEVIDB8_LOG_READER_H
#define LEVIDB8_LOG_READER_H

/*
 * 从硬盘 log 文件读取数据
 */

#include <functional>
#include <memory>

#include "exception.h"
#include "iterator.h"

namespace levidb8 {
    class RandomAccessFile;

    namespace log_reader {
        struct Meta {
            bool compress{};
            bool del{};
        };

        std::unique_ptr<Iterator<Slice/* K */, std::pair<Slice/* V */, Meta>>>
        makeRecordIterator(RandomAccessFile * data_file, uint32_t offset) noexcept;

        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::pair<uint32_t/* offset */, Meta>>>>
        makeTableIterator(RandomAccessFile * data_file) noexcept;

        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::pair<Slice/* V */, Meta>>>>
        makeRecoveryIterator(RandomAccessFile * data_file,
                             std::function<void(const Exception &, uint32_t)> reporter) noexcept;

        bool isRecordDeleted(RandomAccessFile * data_file, uint32_t offset);
    }
}

#endif //LEVIDB8_LOG_READER_H
