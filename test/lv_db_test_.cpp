#include "../src/aggregator/compact_1_2.h"
#include "../src/lv_db.h"

#include <iostream>

void lv_db_test_() {
    std::string db_path = "/tmp/levi_db";
    {
        LeviDB::SeqGenerator seq_gen;
        auto db = std::make_unique<LeviDB::DBSingle>(db_path + "/0", LeviDB::Options{}, &seq_gen);
        LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
        while (compact_db.immut_compacting()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    {
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