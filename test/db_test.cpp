#include <iostream>
#include <vector>

#include "../src/db.h"
#include "../src/env_io.h"

void db_test() {
    const std::string db_name = "/tmp/levi_db";
    if (levidb8::env_io::fileExists(db_name)) {
        levidb8::destroyDB(db_name);
    }

    // 新建数据库
    {
        levidb8::OpenOptions options{};
        try {
            auto db = levidb8::DB::open(db_name, options);
            assert(false);
        } catch (const levidb8::Exception & e) {
            assert(e.isNotFound());
        }
        options.create_if_missing = true;
        auto db = levidb8::DB::open(db_name, options);
    }
    {
        try {
            levidb8::OpenOptions options{};
            options.create_if_missing = true;
            auto db = levidb8::DB::open(db_name, options);
        } catch (const levidb8::Exception & e) {
            assert(e.isInvalidArgument());
        }
        // 正常打开数据库
        auto db = levidb8::DB::open(db_name, {});
        // 逐条写入
        for (size_t i = 0; i < 100; ++i) {
            std::string k = std::to_string(i);
            db->put(k, k, {});
        }
        // 逐条读取
        for (size_t i = 0; i < 100; ++i) {
            std::string k = std::to_string(i);
            auto res = db->get(k, {});
            assert(res.first == k && res.second);
        }
        // 逐条删除数据
        for (int i = -100; i < 100 / 2; ++i) {
            std::string k = std::to_string(i);
            db->remove(k, {});
        }
        // 确认删除成功
        for (size_t i = 0; i < 100 / 2; ++i) {
            std::string k = std::to_string(i);
            auto res = db->get(k, {});
            assert(!res.second);
        }
        for (size_t i = 100 / 2; i < 100; ++i) {
            std::string k = std::to_string(i);
            auto res = db->get(k, {});
            assert(res.first == k && res.second);
        }

        // batch 写入
        std::string k = "100";
        std::string k2 = "101";
        db->write({{k,  k},
                   {k2, k2}}, {});

#define CHECK_DATA \
        auto it = db->scan({}); \
        it->seekToFirst(); \
        for (size_t i = 100; i < 102; ++i) { \
            std::string key = std::to_string(i); \
            assert(it->key() == key); \
            it->next(); \
        } \
        for (size_t i = 50; i < 100; ++i) { \
            std::string key = std::to_string(i); \
            assert(it->key() == key); \
            it->next(); \
        } \
        assert(!it->valid());
        CHECK_DATA;

        // batch 删除
        db->write({{k,  levidb8::Slice::nullSlice()},
                   {k2, "X"}}, {});
        assert(!db->get(k, {}).second);
        assert(db->get(k2, {}).first == "X");
    }
    // 再次打开数据库
    {
        auto db = levidb8::DB::open(db_name, {});
        std::string k = "100";
        std::string k2 = "101";
        assert(!db->get(k, {}).second);
        assert(db->get(k2, {}).first == "X");
        db->put(k, k, {});
        db->put(k2, k2, {});
        CHECK_DATA;
    }
    // simple repair
    {
        levidb8::env_io::deleteFile(db_name + "/keeper");
        auto db = levidb8::DB::open(db_name, {});
        CHECK_DATA;

        std::string k = "1000" + std::string(UINT8_MAX, 'A');
        std::string k2 = "1011" + std::string(UINT8_MAX, 'B');
        db->write({{k,  k},
                   {k2, k2}}, {});
        assert(db->get(k, {}).second);
        assert(db->get(k2, {}).second);
    }
    // repair
    {
        levidb8::repairDB(db_name, [](const levidb8::Exception &, uint32_t) noexcept {});
        auto db = levidb8::DB::open(db_name, {});
        // 确认数据
        auto it = db->scan({});
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            std::string k = std::to_string(i);
            assert(it->key() == k);
            it->next();
        }
    }
    std::cout << __FUNCTION__ << std::endl;
}