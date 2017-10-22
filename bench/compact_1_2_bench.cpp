#ifdef LEVI_BENCH

#include <iostream>

#include "../src/aggregator/compact_1_2.h"
#include "source_fetcher.h"

void compact_1_2_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname_)) {
        const std::string db_name = "/tmp/lv_bench_db";
        for (const auto & name:{db_name + "_a", db_name + "_b"}) {
            if (LeviDB::IOEnv::fileExists(name)) {
                for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                    LeviDB::IOEnv::deleteFile((name + '/') += child);
                }
                LeviDB::IOEnv::deleteDir(name);
            }
        }

        LeviDB::SeqGenerator seq_gen;
        LeviDB::Compacting1To2DB db(std::make_unique<LeviDB::DBSingle>(db_name, LeviDB::Options{}, &seq_gen), &seq_gen);

        SourceFetcher src;
        for (int i = 0; i < test_times_; ++i) {
            auto item = src.readItem();
            db.put(LeviDB::WriteOptions{}, item.first, item.second);
        }

        SourceFetcher src2;
        for (int i = 0; i < test_times_; ++i) {
            auto item = src.readItem();
            db.get(LeviDB::ReadOptions{}, item.first);
        }

        auto it = db.makeIterator(db.makeSnapshot());
        it->seekToFirst();
        while (it->valid()) {
            assert(it->key().size() != 0 && !it->value().empty());
            it->next();
        }

        it->seekToLast();
        std::string target = it->key().toString();
        while (it->valid()) {
            assert(it->key().size() != 0 && !it->value().empty());
            it->prev();
        }

        it->seek(target);
        while (it->valid()) {
            assert(it->key().size() != 0 && !it->value().empty());
            it->prev();
        }

        while (db.immut_compacting()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        std::cout << __FUNCTION__ << std::endl;
    }
}

#endif // LEVI_BENCH