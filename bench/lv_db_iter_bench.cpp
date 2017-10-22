#ifdef LEVI_BENCH

#include <iostream>

#include "../src/lv_db.h"

void lv_db_iter_bench() {
    const std::string db_path = "/tmp/levi_bench_db";
    LeviDB::LvDB db(db_path, LeviDB::Options{});

    std::vector<std::thread> jobs;
    for (int i = 0; i < 2; ++i) {
        jobs.emplace_back([&db]() noexcept {
            try {
                auto it = db.makeIterator(db.makeSnapshot());
                it->seekToFirst();
                while (it->valid()) {
                    it->next();
                }

                it->seekToLast();
                std::string target = it->key().toString();
                while (it->valid()) {
                    it->prev();
                }

                it->seek(target);
                while (it->valid()) {
                    it->prev();
                }
            } catch (const LeviDB::Exception & e) {
                std::cout << e.toString() << std::endl;
            }
        });
    }
    for (std::thread & job:jobs) {
        job.join();
    }

    std::cout << __FUNCTION__ << std::endl;
}

#endif