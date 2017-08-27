#include <iostream>

#include "../src/index_mvcc_rd.h"
#include "../src/log_writer.h"

void index_rd_test() {
    const std::string index_fname = "/tmp/bdt_rd_index";
    const std::string data_fname = "/tmp/bdt_rd_data";
    constexpr int test_times_ = 100;

    if (LeviDB::IOEnv::fileExists(index_fname)) {
        LeviDB::IOEnv::deleteFile(index_fname);
    }
    if (LeviDB::IOEnv::fileExists(data_fname)) {
        LeviDB::IOEnv::deleteFile(data_fname);
    }

    {
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
    }

    std::cout << __FUNCTION__ << std::endl;
}