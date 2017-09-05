#include <iostream>

#ifndef __clang__
#include <algorithm>
#endif

#include "../src/index_iter_regex.h"
#include "../src/log_writer.h"

void index_iter_test() {
    const std::string index_fname = "/tmp/bdt_iter_index";
    const std::string data_fname = "/tmp/bdt_iter_data";
    static constexpr int test_times_ = 100;

    if (LeviDB::IOEnv::fileExists(index_fname)) {
        LeviDB::IOEnv::deleteFile(index_fname);
    }
    if (LeviDB::IOEnv::fileExists(data_fname)) {
        LeviDB::IOEnv::deleteFile(data_fname);
    }

    LeviDB::AppendableFile af(data_fname);
    LeviDB::RandomAccessFile rf(data_fname);

    LeviDB::SeqGenerator seq_g;
    LeviDB::IndexIter index(index_fname, &seq_g, &rf);
    LeviDB::LogWriter writer(&af);

    std::vector<std::string> expect_keys;
    expect_keys.reserve(test_times_);
    for (int i = 0; i < test_times_; ++i) {
        expect_keys.emplace_back(std::to_string(i));
        uint32_t pos = writer.calcWritePos();
        std::vector<uint8_t> b = LeviDB::LogWriter::makeRecord(expect_keys.back(), std::to_string(i + test_times_));
        writer.addRecord({b.data(), b.size()});
        index.insert(expect_keys.back(), LeviDB::OffsetToData{pos});
    }
    std::sort(expect_keys.begin(), expect_keys.end());

    auto verify = [&]() {
        auto iter = index.makeIterator();
        iter->seekToFirst();
        for (const std::string & k:expect_keys) {
            assert(iter->key() == k);
            iter->next();
        }
        assert(!iter->valid());
    };
    verify();

    std::vector<uint8_t> compress_bkvs = LeviDB::LogWriter::makeCompressRecord({{"A", "B"},
                                                                                {"C", "D"},
                                                                                {"E", "F"}});
    uint32_t pos = writer.calcWritePos();
    writer.addCompressRecord({compress_bkvs.data(), compress_bkvs.size()});
    index.insert("A", LeviDB::OffsetToData{pos});
    index.insert("C", LeviDB::OffsetToData{pos});
    index.insert("E", LeviDB::OffsetToData{pos});
    expect_keys.insert(expect_keys.end(), {"A", "C", "E"});
    verify();

    {
        auto iter = index.makeIterator();
        iter->seek("C");
        assert(iter->key() == "C");
        iter->next();
        assert(iter->key() == "E");

        iter->seek("C");
        assert(iter->value()[0] == 'D');
        iter->prev();
        assert(iter->value()[0] == 'B');
        iter->next();
        assert(iter->value()[0] == 'D');

        iter->seekToLast();
        assert(iter->value()[0] == 'F');
        iter->prev();
        assert(iter->value()[0] == 'D');
        iter->prev();
        assert(iter->value()[0] == 'B');
    }

    std::cout << __FUNCTION__ << std::endl;
}