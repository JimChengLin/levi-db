#include "log_reader.h"

namespace LeviDB {
    std::unique_ptr<LogReader::kv_iter>
    LogReader::makeIterator(RandomAccessFile * data_file, uint32_t offset) noexcept {

    }

    std::unique_ptr<SimpleIterator<Slice>>
    LogReader::makeRawIterator(RandomAccessFile * data_file, uint32_t offset) noexcept {

    }
}