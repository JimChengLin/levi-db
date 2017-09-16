#include <iostream>

#include "../src/db_single.h"

void db_single_test() {
    const std::string db_name = "/tmp/lv_db";
    static constexpr int test_times_ = 100;

    if (LeviDB::IOEnv::fileExists(db_name)) {
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
        for (int i = 0; i < test_times_; ++i) {
            db.put(LeviDB::WriteOptions{}, std::to_string(i), {});
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}