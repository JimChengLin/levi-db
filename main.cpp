/*
 * 本项目使用 CMake 开发(Mac)与测试(Mac and Linux)
 * 使用 Makefile 进行生产环境编译
 * 本文件为所有测试的入口
 */

#include <iostream>

void coder_test() noexcept;

void skiplist_test() noexcept;

void repeat_detector_test() noexcept;

void index_test() noexcept;

void crc32c_test() noexcept;

void compressor_test() noexcept;

void log_writer_test() noexcept;

int main() noexcept {
    coder_test();
    skiplist_test();
    repeat_detector_test();
    index_test();
    crc32c_test();
    compressor_test();
    log_writer_test();
    std::cout << "done." << std::endl;
    return 0;
}