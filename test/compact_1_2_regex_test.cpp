#include <iostream>

#include "../src/aggregator/compact_1_2.h"

void compact_1_2_regex_test() {
    std::string db_name = "/tmp/lv_db";

    for (const auto & name:{db_name, db_name + "_a", db_name + "_b"}) {
        if (LeviDB::IOEnv::fileExists(name)) {
            for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                LeviDB::IOEnv::deleteFile((name + '/') += child);
            }
            LeviDB::IOEnv::deleteDir(name);
        }
    }

    typedef LeviDB::Regex::R R;
    auto r_obj = std::make_shared<R>(R("5") << R("0", "9", 0, INT_MAX));

    LeviDB::SeqGenerator seq_gen;
    LeviDB::Options options{};
    options.create_if_missing = true;
    auto db = std::make_unique<LeviDB::DBSingle>(db_name, options, &seq_gen);
    for (int i = 0; i < 100; ++i) {
        db->put(LeviDB::WriteOptions{}, std::to_string(i), std::to_string(i));
    }
    LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
    assert(compact_db.immut_product_a()->canRelease());
    assert(compact_db.immut_product_b()->canRelease());

    std::thread task([it = compact_db.makeRegexIterator(r_obj, compact_db.makeSnapshot())]() noexcept {
        try {
            while (it->valid()) {
                auto item = it->item();
                assert(item.first == item.second && item.second[0] == '5');
                it->next();
            }
        } catch (const LeviDB::Exception & e) {
            std::cout << e.toString() << std::endl;
        }
    });
    task.detach();

    {
        auto it = compact_db.makeRegexReversedIterator(r_obj, compact_db.makeSnapshot());
        while (it->valid()) {
            auto item = it->item();
            assert(item.first == item.second && item.second[0] == '5');
            it->next();
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::thread task_2([it = compact_db.makeRegexIterator(r_obj, compact_db.makeSnapshot())]() noexcept {
        try {
            while (it->valid()) {
                auto item = it->item();
                assert(item.first == item.second && item.second[0] == '5');
                it->next();
            }
        } catch (const LeviDB::Exception & e) {
            std::cout << e.toString() << std::endl;
        }
    });
    task_2.detach();

    while (!compact_db.canRelease()) {
        compact_db.tryApplyPending();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << __FUNCTION__ << std::endl;
}