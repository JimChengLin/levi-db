#pragma once
#ifndef LEVIDB8_LOG_READER_H
#define LEVIDB8_LOG_READER_H

/*
 * 从硬盘 log 文件读取数据
 */

#include <array>
#include <atomic>
#include <functional>
#include <memory>

#include "../include/exception.h"
#include "../include/iterator.h"
#include "config.h"
#include "simple_iterator.h"

namespace levidb8 {
    class RandomAccessFile;

    struct LogMeta {
        bool compress{};
        bool del{};
    };

    struct RecordCache {
        struct DataUnit {
            std::unique_ptr<Iterator<Slice, Slice, LogMeta>> iter;
            uint32_t offset{};
            bool compress{};
        };

        std::array<std::atomic<Iterator<Slice, Slice, LogMeta> *>, kReaderObjCacheNum> normal_obj_cache{};
        std::array<std::atomic<Iterator<Slice, Slice, LogMeta> *>, kReaderObjCacheNum> compress_obj_cache{};
        std::array<std::shared_ptr<DataUnit>, kReaderDataCacheNum> data_cache;

        ~RecordCache() noexcept;
    };

    class RecordIterator : public Iterator<Slice/* K */, Slice/* V */, LogMeta> {
    public:
        static std::unique_ptr<RecordIterator>
        open(RandomAccessFile * data_file, uint32_t offset, RecordCache & cache);
    };

    class TableIterator : public SimpleIterator<std::pair<Slice/* K */, uint32_t/* OffsetToData */>, LogMeta> {
    public:
        static std::unique_ptr<TableIterator>
        open(RandomAccessFile * data_file);
    };

    class RecoveryIterator : public SimpleIterator<std::pair<Slice/* K */, Slice/* V */>, LogMeta> {
    public:
        static std::unique_ptr<RecoveryIterator>
        open(RandomAccessFile * data_file,
             std::function<void(const Exception &, uint32_t/* pos */)> reporter) noexcept;
    };
}

#endif //LEVIDB8_LOG_READER_H
