#ifdef LEVI_BENCH

#include <iostream>

#include "../src/db_single.h"
#include "source_fetcher.h"

void db_bench() {
    if (LeviDB::IOEnv::fileExists(src_fname_)) {
        const std::string db_name = "/tmp/lv_bench_db";
        if (LeviDB::IOEnv::fileExists(db_name)) {
            for (const std::string & child:LeviDB::IOEnv::getChildren(db_name)) {
                LeviDB::IOEnv::deleteFile((db_name + '/') += child);
            }
            LeviDB::IOEnv::deleteDir(db_name);
        }

        LeviDB::SeqGenerator seq_gen;
        LeviDB::Options options{};
        options.create_if_missing = true;
        LeviDB::DBSingle db(db_name, options, &seq_gen);

        SourceFetcher src;
        for (int i = 0; i < test_times_; ++i) {
            auto item = src.readItem();
            db.put(LeviDB::WriteOptions{}, item.first, item.second);
        }

        std::vector<std::unique_ptr<LeviDB::Snapshot>> q;
        for (int i = 0; i < 10; ++i) {
            q.emplace_back(db.makeSnapshot());
            for (int j = 0; j < test_times_ / 10; ++j) {
                auto item = src.readItem();
                db.put(LeviDB::WriteOptions{}, item.first, item.second);
            }
        }

        SourceFetcher src2;
        for (int i = 0; i < test_times_; ++i) {
            auto item = src.readItem();
            auto r = db.get(LeviDB::ReadOptions{}, item.first);
            assert(r.first.size() == item.second.size());
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

        std::cout << __FUNCTION__ << std::endl;
    }
}

#endif // LEVI_BENCH