#ifdef LEVI_BENCH

#include <array>
#include <iostream>

#include "../src/index_mvcc_rd.h"

#define src_fname "/Users/yuanjinlin/Desktop/curr_proj/LeviDB/cmake-build-debug/movies.txt"

void kv_read_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname)) {
        const std::string index_fname = "/tmp/levi_bench_index";
        const std::string data_fname = "/tmp/levi_bench_data";
        if (!LeviDB::IOEnv::fileExists(index_fname) || !LeviDB::IOEnv::fileExists(data_fname)) {
            return;
        }

        LeviDB::SeqGenerator seq_g;
        LeviDB::RandomAccessFile rf(data_fname);
        const LeviDB::IndexRead bdt(index_fname, LeviDB::OffsetToEmpty{LeviDB::IndexConst::disk_null_}, &seq_g, &rf);

        static constexpr int test_time_ = 100000;
        LeviDB::SequentialFile src(src_fname);

        int nth = 0;
        std::array<std::string, 9> que{};
        while (nth++ != test_time_) {
            std::string line = src.readLine();
            if (line.empty()) {
                break;
            }

            size_t idx = (nth - 1) % que.size();
            que[idx] = std::move(line);
            if (idx == que.size() - 1) {
                const std::string key = que[0] + que[1] + std::to_string(nth);
                const std::string val = que[2] + que[3] + que[4] + que[5] + que[6] + que[7];

                auto r = bdt.find(key);
                assert(r.first.size() == val.size());
            }
        }

        std::cout << __FUNCTION__ << std::endl;
    }
}

#endif // LEVI_BENCH