#include <iostream>

#include "../src/index_read.h"
#include "../src/log_writer.h"

void index_read_test() {
    const std::string index_fname = "/tmp/bdt";
    const std::string data_fname = "/tmp/levi_log";
    static constexpr int test_times = 100;

    if (levidb8::env_io::fileExist(index_fname)) {
        levidb8::env_io::deleteFile(index_fname);
    }
    if (levidb8::env_io::fileExist(data_fname)) {
        levidb8::env_io::deleteFile(data_fname);
    }

    levidb8::AppendableFile af(data_fname);
    levidb8::RandomAccessFile rf(data_fname);

    levidb8::BitDegradeTreeRead bdt_read(index_fname, &rf);
    levidb8::LogWriter writer(&af);

    {
        for (size_t i = 0; i < test_times; ++i) {
            std::string k = std::to_string(i);
            std::string v = std::to_string(i + test_times);

            auto bkv = levidb8::LogWriter::makeRecord(k, v);
            uint32_t pos = writer.addRecord(bkv);
            bdt_read.insert(k, levidb8::OffsetToData{pos});
        }

        std::pair<levidb8::Slice, levidb8::Slice> src[] = {{"A", "B"},
                                                           {"C", "D"},
                                                           {"E", "F"}};
        auto bkvs = levidb8::LogWriter::makeCompressedRecords(src, sizeof(src) / sizeof(src[0]));

        uint32_t pos = writer.addCompressedRecords(bkvs);
        bdt_read.insert("A", levidb8::OffsetToData{pos});
        bdt_read.insert("C", levidb8::OffsetToData{pos});
        bdt_read.insert("E", levidb8::OffsetToData{pos});

        std::string out;
        assert((bdt_read.find("A", &out), out == "B"));
        assert((bdt_read.find("C", &out), out == "D"));
        assert((bdt_read.find("E", &out), out == "F"));

        auto bkv = levidb8::LogWriter::makeRecord("A", {});
        pos = writer.addRecordForDel(bkv);
        bdt_read.remove("A", levidb8::OffsetToData{pos});
        bdt_read.remove("A", levidb8::OffsetToData{pos});
        assert(!bdt_read.find("A", nullptr));

        auto bkv_a = levidb8::LogWriter::makeRecord("A", "A_");
        auto bkv_b = levidb8::LogWriter::makeRecord("C", "C_");
        levidb8::Slice slices[] = {bkv_a,
                                   bkv_b};
        auto addrs = writer.addRecordsMayDel(slices, sizeof(slices) / sizeof(slices[0]));
        bdt_read.insert("A", levidb8::OffsetToData{addrs.front()});
        bdt_read.insert("C", levidb8::OffsetToData{addrs.back()});
        assert((bdt_read.find("A", &out), out == "A_"));
        assert((bdt_read.find("C", &out), out == "C_"));
    }
    {
        size_t cnt = 0;
        auto iter = bdt_read.scan();
        for (iter->seekToFirst();
             iter->valid();
             iter->next()) {
            assert(iter->key().size() != 0);
            ++cnt;
        }
        assert(cnt == 100 + 3);
    }

    std::cout << __FUNCTION__ << std::endl;
}