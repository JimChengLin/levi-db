#include <iostream>

#include "../src/aggregator/compact_1_2.h"
#include "../src/aggregator/compact_2_1.h"
#include "../src/lv_db.h"

void lv_db_test_() {
    const std::string db_path = "/tmp/levi_db";

    { // 1 to 2
        LeviDB::SeqGenerator seq_gen;
        auto db = std::make_unique<LeviDB::DBSingle>(db_path + "/0", LeviDB::Options{}, &seq_gen);
        LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
        while (compact_db.immut_compacting()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    { // rename, check
        LeviDB::LvDB db(db_path, LeviDB::Options{});
        auto it = db.makeIterator(db.makeSnapshot());
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            assert(it->key().toString() == std::to_string(i));
            it->next();
        }
    }
    { // 2 to 1 success
        LeviDB::SeqGenerator seq_gen;
        LeviDB::Compacting2To1Worker worker(
                std::make_unique<LeviDB::DBSingle>(db_path + "/1", LeviDB::Options{}, &seq_gen),
                std::make_unique<LeviDB::DBSingle>(db_path + "/2", LeviDB::Options{}, &seq_gen),
                &seq_gen);
    }
    { // rename, check
        LeviDB::LvDB db(db_path, LeviDB::Options{});
        auto it = db.makeIterator(db.makeSnapshot());
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            assert(it->key().toString() == std::to_string(i));
            it->next();
        }
    }
    { // 1 to 2
        LeviDB::SeqGenerator seq_gen;
        auto db = std::make_unique<LeviDB::DBSingle>(db_path + "/3", LeviDB::Options{}, &seq_gen);
        LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
        while (compact_db.immut_compacting()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    { // rename
        LeviDB::LvDB db(db_path, LeviDB::Options{});
    }
    { // 2 to 1 fail
        {
            LeviDB::SeqGenerator seq_gen;
            LeviDB::Compacting2To1Worker worker(
                    std::make_unique<LeviDB::DBSingle>(db_path + "/4", LeviDB::Options{}, &seq_gen),
                    std::make_unique<LeviDB::DBSingle>(db_path + "/5", LeviDB::Options{}, &seq_gen),
                    &seq_gen);
        }
        LeviDB::IOEnv::renameFile(db_path + "/4+5-", db_path + "/4+5");
    }
    { // check, rename
        LeviDB::LvDB db(db_path, LeviDB::Options{});
        auto it = db.makeIterator(db.makeSnapshot());
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            assert(it->key().toString() == std::to_string(i));
            it->next();
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}