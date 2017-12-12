#include <iostream>
#include <vector>

#include "../include/db.h"
#include "../src/env_io.h"

void db_test() {
    const std::string db_name = "/tmp/levi_db";
    const std::string tmp_name = db_name + "_tmp";
    if (levidb8::env_io::fileExist(db_name)) {
        levidb8::destroyDB(db_name);
    }
    if (levidb8::env_io::fileExist(tmp_name)) {
        levidb8::destroyDB(tmp_name);
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
            options.error_if_exist = true;
            auto db = levidb8::DB::open(db_name, options);
            assert(false);
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
            auto res = db->get(k);
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
            auto res = db->get(k);
            assert(!res.second);
        }
        for (size_t i = 100 / 2; i < 100; ++i) {
            std::string k = std::to_string(i);
            auto res = db->get(k);
            assert(res.first == k && res.second);
        }

        // batch 写入
        std::string k = "100";
        std::string k2 = "101";
        std::pair<levidb8::Slice, levidb8::Slice> src[] = {{k,  k},
                                                           {k2, k2}};
        db->write(src, sizeof(src) / sizeof(src[0]), {});

#define CHECK_DATA \
        auto it = db->scan(); \
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
        std::pair<levidb8::Slice, levidb8::Slice> source[] = {{k,  levidb8::Slice::nullSlice()},
                                                              {k2, "X"}};
        db->write(source, sizeof(source) / sizeof(source[0]), {});
        assert(!db->get(k).second);
        assert(db->get(k2).first == "X");
        db->sync();
    }
    // 再次打开数据库
    {
        auto db = levidb8::DB::open(db_name, {});
        std::string k = "100";
        std::string k2 = "101";
        assert(!db->get(k).second);
        assert(db->get(k2).first == "X");
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
        std::pair<levidb8::Slice, levidb8::Slice> src[] = {{k,  k},
                                                           {k2, k2}};
        db->write(src, sizeof(src) / sizeof(src[0]), {});
        assert(db->get(k).second);
        assert(db->get(k2).second);
    }
    // repair
    {
        bool res = levidb8::repairDB(db_name, [](const levidb8::Exception & e, uint32_t) noexcept {});
        assert(res);
        auto db = levidb8::DB::open(db_name, {});
        // 确认数据
        auto it = db->scan();
        it->seek("60");
        for (int i = 60; i < 100; ++i) {
            std::string k = std::to_string(i);
            assert(it->key() == k && it->value() == k);
            it->next();
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}