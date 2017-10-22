#ifdef LEVI_BENCH

#include <iostream>

#include "../src/lv_db.h"
#include "source_fetcher.h"

void lv_db_write_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname_)) {
        const std::string db_path = "/tmp/levi_bench_db";
        if (LeviDB::IOEnv::fileExists(db_path)) {
            for (const std::string & single_name:LeviDB::IOEnv::getChildren(db_path)) {
                std::string single_path = (db_path + '/') += single_name;
                if (single_name[0] >= '0' && single_name[0] <= '9') {
                    for (const std::string & c:LeviDB::IOEnv::getChildren(single_path)) {
                        LeviDB::IOEnv::deleteFile((single_path + '/') += c);
                    }
                    LeviDB::IOEnv::deleteDir(single_path);
                } else {
                    LeviDB::IOEnv::deleteFile(single_path);
                }
            }
            LeviDB::IOEnv::deleteDir(db_path);
        }

        LeviDB::Options options{};
        options.create_if_missing = true;
        LeviDB::LvDB db(db_path, options);

        std::vector<std::thread> jobs;
        for (int i = 0; i < 2; ++i) {
            jobs.emplace_back([&db](int n) noexcept {
                try {
                    int cnt = 0;
                    SourceFetcher src;
                    while (true) {
                        auto item = src.readItem();
                        if (item.first.empty()) {
                            break;
                        }
                        if (((cnt++) & 1) == n) {
                            db.put(LeviDB::WriteOptions{}, item.first, item.second);
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