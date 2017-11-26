#include <iostream>

#include "../src/env_io.h"
#include "../src/log_reader.h"
#include "../src/log_writer.h"

void log_test() {
    const std::string fname = "/tmp/levi_log";
    if (levidb8::env_io::fileExists(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    levidb8::AppendableFile af(fname);
    levidb8::LogWriter writer(&af);
    levidb8::RandomAccessFile rf(fname);

    {
        auto bkv = levidb8::LogWriter::makeRecord(std::string(UINT16_MAX, 'A'), {});
        writer.addRecord({bkv.data(), bkv.size()});
        auto iter = levidb8::log_reader::makeRecordIterator(&rf, 1);
        iter->seekToFirst();
        assert(iter->valid() && iter->key().size() == UINT16_MAX);
    }
    {
        auto bkvs = levidb8::LogWriter::makeCompressedRecords({{"A", "B"},
                                                               {"C", "D"},
                                                               {"E", "F"}});
        uint32_t pos = writer.addCompressedRecords({bkvs.data(), bkvs.size()});
        auto kv_iter = levidb8::log_reader::makeRecordIterator(&rf, pos);

        kv_iter->seekToFirst();
        assert(kv_iter->valid());
        assert(kv_iter->key() == "A");
        assert(kv_iter->value().first == "B");

        kv_iter->next();
        assert(kv_iter->key() == "C");
        assert(kv_iter->value().first == "D");

        kv_iter->next();
        assert(kv_iter->key() == "E");
        assert(kv_iter->value().first == "F");

        kv_iter->next();
        assert(!kv_iter->valid());

        kv_iter->seekToLast();
        assert(kv_iter->key() == "E");
        kv_iter->seek("C");
        assert(kv_iter->key() == "C");
        kv_iter->prev();
        assert(kv_iter->key() == "A");
        kv_iter->prev();
        assert(!kv_iter->valid());
    }
    {
        std::string value_input(UINT16_MAX, 'B');
        auto bkv = levidb8::LogWriter::makeRecord("KEY", value_input);
        uint32_t pos = writer.addRecord({bkv.data(), bkv.size()});

        auto iter = levidb8::log_reader::makeRecordIterator(&rf, pos);
        iter->seekToFirst();
        assert(iter->valid());
        assert(iter->key() == "KEY");
        assert(iter->value().first == value_input);
        iter->prev();
        assert(!iter->valid());
    }
    {
        size_t length = 0;
        auto table_iter = levidb8::log_reader::makeTableIterator(&rf);
        table_iter->prepare();
        while (true) {
            table_iter->next();
            if (!table_iter->valid()) {
                break;
            }
            length += table_iter->item().first.size();
        }
        assert(length == UINT16_MAX + 3/* A B C */ + 3/* KEY */);
    }
    {
        levidb8::RandomWriteFile wf(fname);
        {
            char c;
            uint64_t last = levidb8::env_io::getFileSize(fname) - 1;
            rf.read(last, 1, &c);
            wf.write(last, "\x06");
#define SCAN_ALL \
            auto recovery_iter = levidb8::log_reader::makeRecoveryIterator(&rf, \
                                                                           [](const levidb8::Exception &, \
                                                                              uint32_t) noexcept {}); \
            size_t res_cnt = 0; \
            recovery_iter->prepare(); \
            while (true) { \
                recovery_iter->next(); \
                if (!recovery_iter->valid()) { \
                    break; \
                } \
                ++res_cnt; \
            }
            SCAN_ALL;
            assert(res_cnt == 4);
            wf.write(last, {&c, 1});
        }
        {
            wf.write(30000, "\x06");
            SCAN_ALL;
            assert(res_cnt == 4);
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}