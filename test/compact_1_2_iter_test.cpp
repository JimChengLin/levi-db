#include <iostream>

#include "../src/aggregator/compact_1_2.h"

void compact_1_2_iter_test() {
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
    auto db = std::make_unique<LeviDB::DBSingle>(db_name, options, &seq_gen);
    for (int i = 0; i < 100; ++i) {
        db->put(LeviDB::WriteOptions{}, std::to_string(i), std::to_string(i));
    }
    LeviDB::Compacting1To2DB compact_db(std::move(db), &seq_gen);
    assert(compact_db.immut_product_a()->canRelease());
    assert(compact_db.immut_product_b()->canRelease());

    std::thread task([it = compact_db.makeIterator(compact_db.makeSnapshot())]() noexcept {
        try {
            int i = 0;
            it->seekToFirst();
            while (it->valid()) {
                ++i;
                assert(it->key() == it->value());
                it->next();
            }
            assert(i == 100);

            i = 0;
            it->seekToLast();
            while (it->valid()) {
                ++i;
                assert(it->key() == it->value());
                it->prev();
            }
            assert(i == 100);

            i = 0;
            it->seekToLast();
            while (it->valid() && i < 50) {
                ++i;
                assert(it->key() == it->value());
                if (i == 50) {
                    break;
                }
                it->prev();
            }
            while (it->valid()) {
                ++i;
                assert(it->key() == it->value());
                it->next();
            }
            assert(i == 100);

            i = 0;
            it->seekToFirst();
            while (it->valid() && i < 50) {
                ++i;
                assert(it->key() == it->value());
                if (i == 50) {
                    break;
                }
                it->next();
            }
            while (it->valid()) {
                ++i;
                assert(it->key() == it->value());
                it->prev();
            }
            assert(i == 100);
        } catch (const LeviDB::Exception & e) {
            std::cout << e.toString() << std::endl;
        }
    });
    task.detach();

    for (int i = 0; i < 100; i += 2) {
        compact_db.put(LeviDB::WriteOptions{}, std::to_string(i), "#");
    }

    std::thread task_2([it = compact_db.makeIterator(compact_db.makeSnapshot())]() noexcept {
        try {
            int i = 0;
            it->seekToFirst();
            while (it->valid()) {
                ++i;
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
            assert(i == 100);

            i = 0;
            it->seekToLast();
            while (it->valid() && i < 50) {
                ++i;
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                if (i == 50) {
                    break;
                }
                it->prev();
            }
            while (it->valid()) {
                ++i;
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->next();
            }
            assert(i == 100);

            i = 0;
            it->seekToFirst();
            while (it->valid() && i < 50) {
                ++i;
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                if (i == 50) {
                    break;
                }
                it->next();
            }
            while (it->valid()) {
                ++i;
                const std::string k = it->key().toString();
                const std::string v = it->value();
                if (k == v) {
                    assert((std::stoi(k) & 1) == 1);
                } else {
                    assert(v == "#");
                }
                it->prev();
            }
            assert(i == 100);
        } catch (const LeviDB::Exception & e) {
            std::cout << e.toString() << std::endl;
        }
    });
    task_2.detach();

    while (!compact_db.canRelease()) {
        compact_db.tryApplyPending();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    {
        int i = 0;
        auto it = compact_db.makeIterator(compact_db.makeSnapshot());
        it->seekToFirst();
        while (it->valid()) {
            ++i;
            const std::string k = it->key().toString();
            const std::string v = it->value();
            if (k == v) {
                assert((std::stoi(k) & 1) == 1);
            } else {
                assert(v == "#");
            }
            it->next();
        }
        assert(i == 100);
    }

    std::cout << __FUNCTION__ << std::endl;
}