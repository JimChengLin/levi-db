#ifdef LEVI_BENCH

#include <array>
#include <iostream>

#include "../src/index_mvcc_rd.h"
#include "../src/log_writer.h"

#define src_fname "/Users/yuanjinlin/Desktop/curr_proj/LeviDB/cmake-build-debug/movies.txt"

void kv_write_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname)) {
        const std::string index_fname = "/tmp/levi_bench_index";
        const std::string data_fname = "/tmp/levi_bench_data";
        if (LeviDB::IOEnv::fileExists(index_fname)) {
            LeviDB::IOEnv::deleteFile(index_fname);
        }
        if (LeviDB::IOEnv::fileExists(data_fname)) {
            LeviDB::IOEnv::deleteFile(data_fname);
        }

        LeviDB::AppendableFile af(data_fname);
        LeviDB::RandomAccessFile rf(data_fname);

        LeviDB::SeqGenerator seq_g;
        LeviDB::IndexRead bdt(index_fname, &seq_g, &rf);
        LeviDB::LogWriter writer(&af);

        constexpr int test_time_ = 100000;
        LeviDB::SequentialFile src(src_fname);

        int nth = 0;
        std::array<std::string, 9> que{};
        while (nth++ != test_time_) {
            std::string line = src.readLine();
            if (line.empty()) {
                break;
            }

            size_t idx = nth % que.size();
            que[idx] = std::move(line);
            if (idx == que.size() - 1) {
                const std::string key = que[0] + que[1];
                const std::string val = que[2] + que[3] + que[4] + que[5] + que[6] + que[7];

                uint32_t pos = writer.calcWritePos();
                std::vector<uint8_t> b = LeviDB::LogWriter::makeRecord(key, val);
                writer.addRecord({b.data(), b.size()});
                bdt.insert(key, LeviDB::OffsetToData{pos});
            }
        }

        std::cout << __FUNCTION__ << std::endl;
    }
}

#endif // LEVI_BENCH