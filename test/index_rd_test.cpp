#include <iostream>

#include "../src/index_mvcc_rd.h"
#include "../src/log_writer.h"

void index_rd_test() {
    const std::string index_fname = "/tmp/bdt_rd_index";
    const std::string data_fname = "/tmp/bdt_rd_data";
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
    LeviDB::IndexRead bdt_rd(index_fname, &seq_g, &rf);
    LeviDB::LogWriter writer(&af);

    for (int i = 0; i < test_times_; ++i) {
        uint32_t pos = writer.calcWritePos();
        std::vector<uint8_t> b = LeviDB::LogWriter::makeRecord(std::to_string(i), std::to_string(i + test_times_));
        writer.addRecord({b.data(), b.size()});
        bdt_rd.insert(std::to_string(i), LeviDB::OffsetToData{pos});

        for (int j = 0; j <= i; ++j) {
            auto r = bdt_rd.find(std::to_string(j));
            assert(r.first == std::to_string(j + test_times_));
        }
    }

    std::vector<uint8_t> bin = LeviDB::LogWriter::makeCompressRecord({{"A", "B"},
                                                                      {"C", "D"},
                                                                      {"E", "F"}});
    uint32_t pos = writer.calcWritePos();
    writer.addCompressRecord({bin.data(), bin.size()});
    bdt_rd.insert("A", LeviDB::OffsetToData{pos});
    bdt_rd.insert("C", LeviDB::OffsetToData{pos});
    bdt_rd.insert("E", LeviDB::OffsetToData{pos});
    bdt_rd.remove("A");
    bdt_rd.remove("A");

    assert(!bdt_rd.find("A").second);
    auto a = bdt_rd.find("E");
    assert(a.first.front() == 'F' && a.second);

    bin = LeviDB::LogWriter::makeRecord("C", "");
    pos = writer.calcWritePos();
    writer.addDelRecord({bin.data(), bin.size()});

    assert(bdt_rd.find("C").second);
    bdt_rd.insert("C", LeviDB::OffsetToData{pos});
    assert(!bdt_rd.find("C").second);

    std::vector<uint8_t> bin_batch_a = LeviDB::LogWriter::makeRecord("A", "A_");
    std::vector<uint8_t> bin_batch_b = LeviDB::LogWriter::makeRecord("C", "C_");
    std::vector<uint32_t> addrs = writer.addRecords({{bin_batch_a.data(), bin_batch_a.size()},
                                                     {bin_batch_b.data(), bin_batch_b.size()}});
    bdt_rd.insert("A", LeviDB::OffsetToData{addrs.front()});
    bdt_rd.insert("C", LeviDB::OffsetToData{addrs.back()});

    auto b = bdt_rd.find("A");
    assert(b.first == "A_");
    auto c = bdt_rd.find("C");
    assert(c.first == "C_");

    std::cout << __FUNCTION__ << std::endl;
}