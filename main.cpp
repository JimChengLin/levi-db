/*
 * 本项目使用 CMake 开发(Mac)与测试(Mac && Linux)
 * 使用 Makefile 进行生产环境编译
 * 本文件为所有测试的入口
 */

#include <iostream>

void coder_test();

int main() {
    coder_test();
    std::cout << "Done." << std::endl;
    return 0;
}