#include <iostream>

#include "../src/aggregator/compact_1_2.h"

void compact_1_2_test() {
    std::string db_name = "/tmp/lv_db";

    for (const auto & name:{db_name, db_name + "_a", db_name + "_b"}) {
        if (LeviDB::IOEnv::fileExists(name)) {
            for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                LeviDB::IOEnv::deleteFile((name + '/') += child);
            }
            LeviDB::IOEnv::deleteDir(name);
        }
    }

    LeviDB::SeqGenerator seq_gen;
    LeviDB::Options options{};
    options.create_if_missing = true;
    options.compression = false;
    auto db = std::make_unique<LeviDB::DBSingle>(db_name, options, &seq_gen);
    for (int i = 0; i < 100; ++i) {
        db->put(LeviDB::WriteOptions{}, std::to_string(i), std::to_string(i));
    }

    {
        // 分裂
        LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
        {
            auto snapshot = seq_gen.makeSnapshot();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        assert(compact_db.get(LeviDB::ReadOptions{}, "1").first == "1");
        for (int i = 0; i < 100; i += 2) {
            compact_db.put(LeviDB::WriteOptions{}, std::to_string(i), "#");
        }
        compact_db.remove(LeviDB::WriteOptions{}, "0");
        compact_db.remove(LeviDB::WriteOptions{}, "99");
        compact_db.explicitRemove(LeviDB::WriteOptions{}, "1");
        compact_db.explicitRemove(LeviDB::WriteOptions{}, "98");
        while (compact_db.immut_compacting()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        assert(compact_db.canRelease());
        compact_db.tryApplyPending();
        compact_db.updateKeyRange();
        assert(compact_db.largestKey() == "97");
        assert(compact_db.smallestKey() == "10");

        // 确认数据
        const auto & db_a = compact_db.immut_product_a();
        {
            auto it = db_a->makeIterator(db_a->makeSnapshot());
            it->seekToFirst();
            while (it->valid()) {
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
        }
        const auto & db_b = compact_db.immut_product_b();
        {
            auto it = db_b->makeIterator(db_b->makeSnapshot());
            it->seekToFirst();
            while (it->valid()) {
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
        }

        compact_db.write(LeviDB::WriteOptions{}, {{"0", "#"},
                                                  {"1", "1"}});
        compact_db.write(LeviDB::WriteOptions{}, {{"1",  "3"},
                                                  {"97", "3"}});
        for (int i = 0; i < 98; ++i) {
            auto res = compact_db.get(LeviDB::ReadOptions{}, std::to_string(i));
            if (i == 1 || i == 97) {
                assert(res.first == "3");
                continue;
            }
            if (std::to_string(i) == res.first) {
                assert((i & 1) == 1);
            } else {
                assert(res.first == "#");
            }
        }
    }
    {
        for (const auto & name:{db_name + "_a_temp", db_name + "_b_temp"}) {
            if (LeviDB::IOEnv::fileExists(name)) {
                for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                    LeviDB::IOEnv::deleteFile((name + '/') += child);
                }
                LeviDB::IOEnv::deleteDir(name);
            }
        }

        LeviDB::repairCompacting1To2DB(db_name, [](const LeviDB::Exception & e) noexcept {
            std::cout << "RepairCompacting1To2DBTest: " << e.toString() << std::endl;
        });

        LeviDB::DBSingle product_a(db_name + "_a", LeviDB::Options{}, &seq_gen);
        {
            auto it = product_a.makeIterator(product_a.makeSnapshot());
            it->seekToFirst();
            while (it->valid()) {
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == "1") {
                    assert(v == "3");
                } else if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
        }

        LeviDB::DBSingle product_b(db_name + "_b", LeviDB::Options{}, &seq_gen);
        {
            auto it = product_b.makeIterator(product_b.makeSnapshot());
            it->seekToFirst();
            while (it->valid()) {
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == "97") {
                    assert(v == "3");
                } else if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}