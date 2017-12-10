#include <iostream>

#include "../src/env_io.h"
#include "../src/log_reader.h"
#include "../src/log_writer.h"

void log_test() {
    const std::string fname = "/tmp/levi_log";
    if (levidb8::env_io::fileExist(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    levidb8::AppendableFile af(fname);
    levidb8::LogWriter writer(&af);
    levidb8::RandomAccessFile rf(fname);
    levidb8::RecordCache cache;

    {
        auto bkv = levidb8::LogWriter::makeRecord(std::string(UINT16_MAX, 'A'), {});
        writer.addRecord(bkv);
        auto iter = levidb8::RecordIterator::open(&rf, 1, cache);
        iter->seekToFirst();
        assert(iter->valid() && iter->key().size() == UINT16_MAX);
    }
    {
        std::pair<levidb8::Slice, levidb8::Slice> src[] = {{"A", "B"},
                                                           {"C", "D"},
                                                           {"E", "F"}};
        auto bkvs = levidb8::LogWriter::makeCompressedRecords(src, sizeof(src) / sizeof(src[0]));
        uint32_t pos = writer.addCompressedRecords(bkvs);
        auto kv_iter = levidb8::RecordIterator::open(&rf, pos, cache);

        kv_iter->seekToFirst();
        assert(kv_iter->valid());
        assert(kv_iter->key() == "A");
        assert(kv_iter->value() == "B");

        kv_iter->next();
        assert(kv_iter->key() == "C");
        assert(kv_iter->value() == "D");

        kv_iter->next();
        assert(kv_iter->key() == "E");
        assert(kv_iter->value() == "F");

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
        uint32_t pos = writer.addRecord(bkv);

        auto iter = levidb8::RecordIterator::open(&rf, pos, cache);
        iter->seekToFirst();
        assert(iter->valid());
        assert(iter->key() == "KEY");
        assert(iter->value() == value_input);
        iter->prev();
        assert(!iter->valid());
    }
    {
        size_t length = 0;
        auto table_iter = levidb8::TableIterator::open(&rf);
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
        auto levi_scan = [&]() {
            auto recovery_iter = levidb8::RecoveryIterator::open(&rf, [](const levidb8::Exception &,
                                                                         uint32_t) noexcept {});
            size_t res_cnt = 0;
            while (true) {
                recovery_iter->next();
                if (!recovery_iter->valid()) {
                    break;
                }
                ++res_cnt;
            }
            return res_cnt;
        };
        {
            char c;
            uint64_t last = af.immut_length() - 1;
            rf.read(last, 1, &c);
            wf.write(last, "\x06");
            assert(levi_scan() == 4);
            wf.write(last, {&c, 1});
        }
        {
            wf.write(30000, "\x06");
            assert(levi_scan() == 4);
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}