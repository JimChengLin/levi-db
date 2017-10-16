#include <iostream>

#include "../src/lv_db.h"

void lv_db_test() {
    std::string db_path = "/tmp/levi_db";
    if (LeviDB::IOEnv::fileExists(db_path)) {
        for (const std::string & single_name:LeviDB::IOEnv::getChildren(db_path)) {
            std::string single_path = (db_path + '/') += single_name;
            if (single_name[0] >= '0' && single_name[0] <= '9') {
                for (const std::string & c:LeviDB::IOEnv::getChildren(single_path)) {
                    LeviDB::IOEnv::deleteFile((single_path + '/') += c);
                }
                LeviDB::IOEnv::deleteDir(single_path);
            } else {
                LeviDB::IOEnv::deleteFile(single_path);
            }
        }
        LeviDB::IOEnv::deleteDir(db_path);
    }

    // 新建数据库
    {
        LeviDB::Options options{};
        try {
            LeviDB::LvDB db(db_path, options);
            assert(false);
        } catch (const LeviDB::Exception & e) {
            assert(e.isNotFound());
        }

        options.create_if_missing = true;
        LeviDB::LvDB db(db_path, options);
    }
    {
        try {
            LeviDB::Options options{};
            options.error_if_exists = true;
            LeviDB::LvDB db(db_path, options);
            assert(false);
        } catch (const LeviDB::Exception & e) {
            assert(e.isInvalidArgument());
        }

        // 正常打开数据库
        LeviDB::LvDB db(db_path, LeviDB::Options{});

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

        // 快照
        for (int i = 50; i < 60; ++i) {
            db.remove(LeviDB::WriteOptions{}, std::to_string(i));
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

        // 释放 snapshot
        it = nullptr;
        regex_it = nullptr;
        reverse_regex_it = nullptr;

        // batch 删除
        db.write(LeviDB::WriteOptions{}, {{k,  LeviDB::Slice::nullSlice()},
                                          {k2, "X"}});
        assert(!db.get(LeviDB::ReadOptions{}, k).second);
        assert(db.get(LeviDB::ReadOptions{}, k2).first == "X");
    }
    {
        LeviDB::LvDB db(db_path, LeviDB::Options{});
        {
            // 确认数据
            auto it = db.makeIterator(db.makeSnapshot());
            it->seekToFirst();
            assert(it->key() == "101" && it->value() == "X");
            it->next();
            for (int i = 60; i < 100; ++i) {
                assert(it->key() == std::to_string(i));
                it->next();
            }
            assert(!it->valid());
        }

        // compress write
        std::string k = "1000" + std::string(UINT8_MAX, 'A');
        std::string k2 = "1011" + std::string(UINT8_MAX, 'B');
        LeviDB::WriteOptions options{};
        options.compress = true;
        options.uncompress_size = static_cast<uint32_t>(k.size() * 2 + k2.size() * 2);
        db.write(options, {{k,  k},
                           {k2, k2}});
        assert(db.get(LeviDB::ReadOptions{}, k).second);
        assert(db.get(LeviDB::ReadOptions{}, k2).second);
    }
    // repair db
    {
        assert(!LeviDB::repairDB("/PATH_NOT_EXIST", [](const LeviDB::Exception & e) noexcept {}));
        LeviDB::repairDB(db_path, [](const LeviDB::Exception & e) noexcept {
            std::cout << "RepairDBTest: " << e.toString() << std::endl;
        });

        LeviDB::LvDB db(db_path, LeviDB::Options{});
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