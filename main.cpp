/*
 * 本项目使用 CMake 开发(Mac)与测试(Mac and Linux)
 * 使用 Makefile 进行生产环境编译
 * 本文件为所有测试的入口
 */

#include <iostream>

#include "src/exception.h"

void index_test();

void index_mvcc_test();

void compress_test();

void log_test();

void index_rd_test();

void kv_write_bench();

void kv_read_bench();

void index_iter_test();

void regex_test();

void index_regex_test();

void db_single_test();

void misc_test();

void env_io_test();

void compact_1_2_test();

void compact_1_2_iter_test();

void compact_1_2_regex_test();

void compact_2_1_test();

void lv_db_test();

void lv_db_test_();

int main() {
    try {
        index_test();
        index_mvcc_test();
        compress_test();
        log_test();
        index_rd_test();
        index_iter_test();
        regex_test();
        index_regex_test();
        db_single_test();
        misc_test();
        env_io_test();
        compact_1_2_test();
        compact_1_2_iter_test();
        compact_1_2_regex_test();
        compact_2_1_test();
        lv_db_test();
        lv_db_test_();
#ifdef LEVI_BENCH
        kv_write_bench();
        kv_read_bench();
#endif
    } catch (const LeviDB::Exception & e) {
        std::cout << e.toString() << std::endl;
        return 1;
    }
    std::cout << "done." << std::endl;
    return 0;
}