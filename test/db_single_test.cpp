#include <iostream>

#include "../src/db_single.h"

void db_single_test() {
    const std::string db_name = "/tmp/lv_db";

    if (LeviDB::IOEnv::fileExists(db_name)) {
        for (const std::string & child:LeviDB::IOEnv::getChildren(db_name)) {
            LeviDB::IOEnv::deleteFile((db_name + '/') += child);
        }
        LeviDB::IOEnv::deleteDir(db_name);
    }

    // 新建数据库
    {
        LeviDB::SeqGenerator seq_gen;
        LeviDB::Options options{};
        options.create_if_missing = true;
        LeviDB::DBSingle db(db_name, options, &seq_gen);
    }
    {
        // 正常打开数据库
        LeviDB::SeqGenerator seq_gen;
        LeviDB::DBSingle db(db_name, LeviDB::Options{}, &seq_gen);

        // 逐条写入数据
        for (int i = 0; i < 100; ++i) {
            db.put(LeviDB::WriteOptions{}, std::to_string(i), std::to_string(i));
        }

        // 逐条读取数据
        for (int i = 0; i < 100; ++i) {
            auto res = db.get(LeviDB::ReadOptions{}, std::to_string(i));
            assert(res.first == std::to_string(i));
            assert(res.second);
        }

        // 逐条删除数据
        for (int i = -100; i < 100 / 2; ++i) {
            db.remove(LeviDB::WriteOptions{}, std::to_string(i));
        }

        // 确认删除成功
        for (int i = 0; i < 100 / 2; ++i) {
            auto res = db.get(LeviDB::ReadOptions{}, std::to_string(i));
            assert(!res.second);
        }
        for (int i = 100 / 2; i < 100; ++i) {
            auto res = db.get(LeviDB::ReadOptions{}, std::to_string(i));
            assert(res.first == std::to_string(i));
            assert(res.second);
        }

        // batch 写入
        std::string k = "100";
        std::string k2 = "101";
        db.write(LeviDB::WriteOptions{}, {{k,  k},
                                          {k2, k2}});

        // 迭代器
        auto it = db.makeIterator(db.makeSnapshot());
        it->seekToFirst();
        for (int i = 100; i < 102; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        for (int i = 50; i < 100; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        assert(!it->valid());

        // 快照以及显式删除
        for (int i = 50; i < 60; ++i) {
            db.explicitRemove(LeviDB::WriteOptions{}, std::to_string(i));
        }
        assert(!db.get(LeviDB::ReadOptions{}, "51").second);
        it->seekToFirst();
        for (int i = 100; i < 102; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        for (int i = 50; i < 100; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        assert(!it->valid());

        // 正向正则
        typedef LeviDB::Regex::R R;
        auto snapshot = db.makeSnapshot();
        auto snapshot_copy = std::make_unique<LeviDB::Snapshot>(snapshot->immut_seq_num());
        auto regex_it = db.makeRegexIterator(std::make_shared<R>(R("1") << R("0", "9", 0, INT_MAX)),
                                             std::move(snapshot));
        for (int i = 100; i < 102; ++i) {
            assert(regex_it->item().second == std::to_string(i));
            regex_it->next();
        }
        assert(!regex_it->valid());

        // 反向正则
        auto reverse_regex_it = db.makeRegexReversedIterator(std::make_shared<R>(R("1") << R("0", "9", 0, INT_MAX)),
                                                             std::move(snapshot_copy));
        for (int i = 101; i >= 100; --i) {
            assert(reverse_regex_it->item().second == std::to_string(i));
            reverse_regex_it->next();
        }
        assert(!reverse_regex_it->valid());

        // 释放 snapshot 以 apply pending
        it = nullptr;
        regex_it = nullptr;
        reverse_regex_it = nullptr;
        db.tryApplyPending();

        // key range
        assert(db.smallestKey() == "0");
        assert(db.largestKey() == "99");
    }
    // 再次打开数据库
    {
        LeviDB::SeqGenerator seq_gen;
        LeviDB::DBSingle db(db_name, LeviDB::Options{}, &seq_gen);

        // meta keeper
        assert(db.smallestKey() == "0");
        assert(db.largestKey() == "99");

        // 确认数据
        auto it = db.makeIterator(db.makeSnapshot());
        it->seekToFirst();
        for (int i = 100; i < 102; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        for (int i = 60; i < 100; ++i) {
            assert(it->key() == std::to_string(i));
            it->next();
        }
        assert(!it->valid());
    }
    // simple repair
    {
        LeviDB::IOEnv::deleteFile(db_name + "/keeper");

        LeviDB::SeqGenerator seq_gen;
        LeviDB::DBSingle db(db_name, LeviDB::Options{}, &seq_gen);

        {
            // 确认数据
            auto it = db.makeIterator(db.makeSnapshot());
            it->seekToFirst();
            for (int i = 100; i < 102; ++i) {
                assert(it->key() == std::to_string(i));
                it->next();
            }
            for (int i = 60; i < 100; ++i) {
                assert(it->key() == std::to_string(i));
                it->next();
            }
            assert(!it->valid());
        }

        // compress write
        std::string k = "1000" + std::string(UINT8_MAX, 'A');
        std::string k2 = "1011" + std::string(UINT8_MAX, 'B');
        LeviDB::WriteOptions options;
        options.compress = true;
        options.uncompress_size = static_cast<uint32_t>(k.size() * 2 + k2.size() * 2);
        db.write(options, {{k,  k},
                           {k2, k2}});
        assert(db.get(LeviDB::ReadOptions{}, k).second);
        assert(db.get(LeviDB::ReadOptions{}, k2).second);
    }
    // repair single db
    {
        LeviDB::repairDBSingle(db_name, [](const LeviDB::Exception & e) noexcept {
            std::cout << e.toString() << std::endl;
        });

        LeviDB::SeqGenerator seq_gen;
        LeviDB::DBSingle db(db_name, LeviDB::Options{}, &seq_gen);

        // 确认数据
        auto it = db.makeIterator(db.makeSnapshot());
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            assert(it->key().toString() == std::to_string(i));
            it->next();
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}