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
#include "slice.h"

namespace LeviDB {
    namespace LogReader {
        using kv_iter = Iterator<Slice, std::string>;

        // 如果没有传入 reporter, 直接抛出异常
        // 除非在 recovery, 大部分情况下这都是合理的
        // 自定义 reporter 捕获 IOError 时也应继续抛出
        using reporter_t = std::function<void(const Exception &)>;

        [[noreturn]] void defaultReporter(const Exception & e);

        // 注意: const 方法不线程安全(buffer is mutable)
        // 重复 seek 有优化
        // 结尾为 0 == del
        std::unique_ptr<kv_iter>
        makeIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter = defaultReporter);

        // 结尾 == type(std::bitset<8>)
        std::unique_ptr<SimpleIterator<Slice>>
        makeRawIterator(RandomAccessFile * data_file, uint32_t offset, reporter_t reporter = defaultReporter);

        // V.back() == del
        // 这里的 reporter 不应该抛出异常, 而是打日志
        std::unique_ptr<SimpleIterator<std::pair<Slice/* K */, std::string/* V */>>>
        makeTableIterator(RandomAccessFile * data_file, reporter_t reporter = defaultReporter);
    };
}

#endif //LEVIDB_LOG_READER_H