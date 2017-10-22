#ifndef LEVIDB_SOURCE_FETCHER_H
#define LEVIDB_SOURCE_FETCHER_H
#ifdef LEVI_BENCH

#include <array>

#include "../src/env_io.h"

constexpr char src_fname_[] = "/Users/yuanjinlin/Desktop/curr_proj/LeviDB/cmake-build-debug/movies.txt";
constexpr int test_times_ = 10000;

class SourceFetcher {
private:
    std::array<std::string, 9> _que;
    LeviDB::SequentialFile _src;
    int _nth = 0;

public:
    SourceFetcher() : _src(src_fname_) {}

    std::pair<std::string, std::string>
    readItem();
};

#endif // LEVI_BENCH
#endif //LEVIDB_SOURCE_FETCHER_H