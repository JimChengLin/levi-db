#ifdef LEVI_BENCH

#include <iostream>

#include "../src/lv_db.h"
#include "source_fetcher.h"

void lv_db_read_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname_)) {
        const std::string db_path = "/tmp/levi_bench_db";
        const LeviDB::LvDB db(db_path, LeviDB::Options{});

        std::vector<std::thread> jobs;
        for (int i = 0; i < 8; ++i) {
            jobs.emplace_back([&db](int offset) noexcept {
                try {
                    int cnt = 0;
                    SourceFetcher src;
                    while (true) {
                        auto item = src.readItem();
                        if (item.first.empty()) {
                            break;
                        }
                        if ((cnt++) % 8 == offset) {
                            db.get(LeviDB::ReadOptions{}, item.first);
                        }
                    }
                } catch (const LeviDB::Exception & e) {
                    std::cout << e.toString() << std::endl;
                }
            }, i);
        }
        for (std::thread & job:jobs) {
            job.join();
        }

        std::cout << __FUNCTION__ << std::endl;
    }
}

#endif