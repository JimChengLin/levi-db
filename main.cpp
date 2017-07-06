/*
 * 本项目使用 CMake 开发(Mac)与测试(Mac and Linux)
 * 使用 Makefile 进行生产环境编译
 * 本文件为所有测试的入口
 */

#include <iostream>

void coder_test();

void skiplist_test();

void repeat_detector_test();

int main() {
    coder_test();
    skiplist_test();
    repeat_detector_test();
    std::cout << "done." << std::endl;
    return 0;
}