#include <iostream>

#include "src/exception.h"

void index_test();

void index_thread_test();

void index_iter_test();

void index_iter_thread_test();

void compress_test();

void log_test();

void index_read_test();

void db_test();

void db_bench();

int main() {
    try {
#ifndef LEVI_BENCH
        index_test();
        index_thread_test();
        index_iter_test();
        index_iter_thread_test();
        compress_test();
        log_test();
        index_read_test();
        db_test();
#else
        db_bench();
#endif
    } catch (const levidb8::Exception & e) {
        std::cout << e.toString() << std::endl;
        return 1;
    } catch (const std::exception &) {
        return 1;
    }
    return 0;
}