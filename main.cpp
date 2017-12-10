#include <iostream>

#include "include/exception.h"

void compress_test();

void db_bench();

void db_test();

void index_iter_test();

void index_iter_thread_test();

void index_read_test();

void index_test();

void index_thread_test();

void log_test();

int main() {
    try {
#ifndef LEVI_BENCH
        compress_test();
        db_test();
        index_iter_test();
        index_iter_thread_test();
        index_read_test();
        index_test();
        index_thread_test();
        log_test();
#else
        db_bench();
#endif
    } catch (const levidb8::Exception & e) {
        std::cout << e.toString() << std::endl;
        return 1;
    } catch (const std::exception & e) {
        return 1;
    }
    std::cout << "done." << std::endl;
    return 0;
}