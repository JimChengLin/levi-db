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

int main() {
    try {
//        using ms = std::chrono::milliseconds;
//        auto start = std::chrono::high_resolution_clock::now();
        index_test();
//        auto end = std::chrono::high_resolution_clock::now();
//        std::cout << "took "
//                  << std::chrono::duration_cast<ms>(end - start).count()
//                  << " milliseconds" << std::endl;
        index_thread_test();
        index_iter_test();
        index_iter_thread_test();
        compress_test();
        log_test();
        index_read_test();
        db_test();
    } catch (const levidb8::Exception & e) {
        std::cout << e.toString() << std::endl;
        return 1;
    }
    return 0;
}