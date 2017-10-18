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
        it->prev();
        it->next();
        for (int i = 60; i < 100; ++i) {
            assert(it->key().toString() == std::to_string(i));
            it->next();
        }
    }
    { // gc
        {
            LeviDB::SeqGenerator seq_gen;
            auto db = std::make_unique<LeviDB::DBSingle>(db_path + "/5", LeviDB::Options{}, &seq_gen);
            LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
            while (compact_db.immut_compacting()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        LeviDB::LvDB db(db_path, LeviDB::Options{});

        { // cross-db regex iter
            // 正向
            typedef LeviDB::Regex::R R;
            auto snapshot = db.makeSnapshot();
            auto snapshot_copy = std::make_unique<LeviDB::Snapshot>(snapshot->immut_seq_num());
            auto regex_it = db.makeRegexIterator(std::make_shared<R>((R("7") | R("8")) << R("0", "9", 0, INT_MAX)),
                                                 std::move(snapshot));

            int i = 0;
            while (regex_it->valid()) {
                ++i;
                regex_it->next();
            }
            assert(i == 20);

            // 反向
            auto reverse_regex_it = db.makeRegexReversedIterator(
                    std::make_shared<R>((R("1") | R("7")) << R("0", "9", 0, INT_MAX)),
                    std::move(snapshot_copy));

            i = 0;
            while (reverse_regex_it->valid()) {
                ++i;
                reverse_regex_it->next();
            }
            assert(i == 11);
        }

        std::string k = "0_Jim";
        std::string v = "Birthday";
        std::string k2 = "7_1995";
        std::string v2 = "0207";
        db.write(LeviDB::WriteOptions{}, {{k,  v},
                                          {k2, v2}});

        for (int i = 60; i < 100; ++i) {
            assert(db.get(LeviDB::ReadOptions{}, std::to_string(i)).first == std::to_string(i));
        }
        {
            auto it = db.makeIterator(db.makeSnapshot());
            it->seekToFirst();
            it->next();
            it->prev();
            int i = 0;
            while (it->valid()) {
                ++i;
                assert(!it->value().empty());
                it->next();
            }
            assert(i == 45);
        }

        std::string k3 = "9_Happy";
        std::string v3 = "Everyday";
        db.write(LeviDB::WriteOptions{}, {{k,  v},
                                          {k2, v2},
                                          {k3, v3}});

        {
            auto it = db.makeIterator(db.makeSnapshot());
            it->seekToLast();
            it->prev();
            it->next();
            int i = 0;
            while (it->valid()) {
                ++i;
                assert(!it->value().empty());
                it->prev();
            }
            assert(i == 46);
        }
        assert(!db.getProperty().empty());
    }

    std::cout << __FUNCTION__ << std::endl;
}