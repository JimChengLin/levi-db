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

int main() {
    try {
        index_test();
        index_mvcc_test();
        compress_test();
        log_test();
        index_rd_test();
    } catch (const LeviDB::Exception & e) {
        std::cout << e.toString() << std::endl;
        return -1;
    }
    std::cout << "done." << std::endl;
    return 0;
}