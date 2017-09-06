#ifndef LEVIDB_LOG_READER_H
#define LEVIDB_LOG_READER_H

/*
 * 从硬盘 log 文件读取数据
 * 为了保持接口的简洁, item 的最后一位作为 meta
 */

#include <functional>
#include <memory>
#include <string>

#include "env_io.h"
#include "exception.h"
#include "iterator.h"

namespace LeviDB {
    namespace LogReader {
        using kv_iter_t = Iterator<Slice, std::string>;

        using reporter_t = std::function<void(const Exception &)>;

        [[noreturn]] void defaultReporter(const Exception & e);

        // 注意: const 方法不线程安全(buffer is mutable)
        // 结尾 == del
        std::unique_ptr<kv_iter_t>
        makeIterator(RandomAccessFile * data_file, uint32_t offset);

        bool isRecordIteratorCompress(kv_iter_t * it) noexcept;

        // 结尾 == type(std::bitset<8>)
        std::unique_ptr<SimpleIterator<Slice>>
        makeRawIterator(RandomAccessFile * data_file, uint32_t offset);

        // 结尾 == del
        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::string/* V */>>>
        makeTableIterator(RandomAccessFile * data_file);

        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, uint32_t/* offset */>>>
        makeTableIteratorOffset(RandomAccessFile * data_file);

        // 传入的 reporter 不应该继续抛出异常, 而是写日志
        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, uint32_t/* offset */>>>
        makeTableRecoveryIterator(RandomAccessFile * data_file, reporter_t reporter = defaultReporter) noexcept;
    };
}

#endif //LEVIDB_LOG_READER_H