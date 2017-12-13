#if defined(LEVI_BENCH)

#include <array>
#include <iostream>
#include <rocksdb/db.h>
#include <thread>

#include "../include/db.h"
#include "../src/env_io.h"

/*
 * Linux 清空硬盘缓存命令:
 * echo 3 | sudo tee /proc/sys/vm/drop_caches
 */

static constexpr char src_fname[] = "/Users/yuanjinlin/Desktop/movies.txt"; // 修改成正确的测试数据路径
static constexpr char levidb_name[] = "/tmp/levi_bench_db";
static constexpr char rocksdb_name[] = "/tmp/rocks_bench_db";

static constexpr int repeat_times = 3;
static constexpr int test_times = 100000;
static constexpr int thread_num = 8;

/*
 * 读取亚马逊电影评论, 每8行内容加1行空格为1组数据
 */
class Provider {
private:
    levidb8::SequentialFile _src;
    std::array<std::string, 9> _q;
    size_t _nth = 0;

public:
    Provider() : _src(src_fname) {};

    std::pair<std::string, std::string>
    readItem() {
        for (size_t i = 0; i < _q.size(); ++i) {
            _q[i] = _src.readLine().toString();
        }
        std::string k = std::to_string(_nth++) + _q[0] + _q[1];
        if (k.size() > UINT8_MAX) {
            k.resize(UINT8_MAX);
        }
        return {std::move(k), _q[2] + _q[3] + _q[4] + _q[5] + _q[6] + _q[7]};
    };

    void skipItem() {
        for (size_t i = 0; i < _q.size(); ++i) {
            _src.readLine();
        }
        ++_nth;
    }
};

void levidb_write_bench() {
    const std::string db_name = levidb_name;
    if (levidb8::env_io::fileExist(db_name)) {
        levidb8::destroyDB(db_name);
    }

    levidb8::OpenOptions options{};
    options.create_if_missing = true;
    auto db = levidb8::DB::open(db_name, options);

    std::vector<std::thread> jobs;
    for (size_t i = 0; i < thread_num; ++i) {
        jobs.emplace_back([&db](size_t n) {
            Provider src;
            for (size_t j = 0; j < test_times; ++j) {
                if (j % thread_num == n) {
                    auto item = src.readItem();
                    db->put(item.first, item.second, {});
                } else {
                    src.skipItem();
                }
            }
        }, i);
    }

    for (auto & job:jobs) {
        job.join();
    }
}

void rocksdb_write_bench() {
    const std::string db_name = rocksdb_name;
    if (levidb8::env_io::fileExist(db_name)) {
        rocksdb::DestroyDB(db_name, {});
    }
    assert(!levidb8::env_io::fileExist(db_name));

    rocksdb::DB * db;
    rocksdb::Options options{};
    options.create_if_missing = true;
    auto s = rocksdb::DB::Open(options, db_name, &db);
    assert(s.ok());

    std::vector<std::thread> jobs;
    for (size_t i = 0; i < thread_num; ++i) {
        jobs.emplace_back([db](size_t n) {
            Provider src;
            for (size_t j = 0; j < test_times; ++j) {
                if (j % thread_num == n) {
                    auto item = src.readItem();
                    db->Put({}, item.first, item.second);
                } else {
                    src.skipItem();
                }
            }
        }, i);
    }

    for (auto & job:jobs) {
        job.join();
    }
    delete db;
}

void levidb_read_bench() {
    const std::string db_name = levidb_name;
    auto db = levidb8::DB::open(db_name, {});

    std::vector<std::thread> jobs;
    for (size_t i = 0; i < thread_num; ++i) {
        jobs.emplace_back([&db](size_t n) {
            Provider src;
            for (size_t j = 0; j < test_times; ++j) {
                if (j % thread_num == n) {
                    auto item = src.readItem();
                    auto res = db->get(item.first);
                    assert(res.first == item.second);
                } else {
                    src.skipItem();
                }
            }
        }, i);
    }

    for (auto & job:jobs) {
        job.join();
    }
}

void rocksdb_read_bench() {
    const std::string db_name = rocksdb_name;
    rocksdb::DB * db;
    auto s = rocksdb::DB::Open({}, db_name, &db);
    assert(s.ok());

    std::vector<std::thread> jobs;
    for (size_t i = 0; i < thread_num; ++i) {
        jobs.emplace_back([db](size_t n) {
            Provider src;
            for (size_t j = 0; j < test_times; ++j) {
                if (j % thread_num == n) {
                    auto item = src.readItem();
                    std::string res;
                    db->Get({}, item.first, &res);
                    assert(res == item.second);
                } else {
                    src.skipItem();
                }
            }
        }, i);
    }

    for (auto & job:jobs) {
        job.join();
    }
    delete db;
}

void levidb_scan_bench() {
    const std::string db_name = levidb_name;
    auto db = levidb8::DB::open(db_name, {});

    auto it = db->scan();
    size_t cnt = 0;
    for (it->seekToFirst();
         it->valid();
         it->next()) {
        cnt += it->key().size() + it->value().size();
    }
    std::cout << "levidb scan " << cnt << std::endl;
}

void rocksdb_scan_bench() {
    const std::string db_name = rocksdb_name;
    rocksdb::DB * db;
    auto s = rocksdb::DB::Open({}, db_name, &db);
    assert(s.ok());

    auto it = db->NewIterator({});
    size_t cnt = 0;
    for (it->SeekToFirst();
         it->Valid();
         it->Next()) {
        cnt += it->key().size() + it->value().size();
    }
    delete it;
    delete db;
    std::cout << "rocksdb scan " << cnt << std::endl;
}

using ms = std::chrono::milliseconds;

#define PRINT_TIME(name) \
std::cout << #name " took " << std::chrono::duration_cast<ms>(end - start).count() << " milliseconds" << std::endl;

void write_bench() {
    auto start = std::chrono::high_resolution_clock::now();
    levidb_write_bench();
    auto end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(levidb_write_bench);

    start = std::chrono::high_resolution_clock::now();
    rocksdb_write_bench();
    end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(rocksdb_write_bench);
}

void read_bench() {
    auto start = std::chrono::high_resolution_clock::now();
    levidb_read_bench();
    auto end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(levidb_read_bench);

    start = std::chrono::high_resolution_clock::now();
    rocksdb_read_bench();
    end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(rocksdb_read_bench);
}

void scan_bench() {
    auto start = std::chrono::high_resolution_clock::now();
    levidb_scan_bench();
    auto end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(levidb_scan_bench);

    start = std::chrono::high_resolution_clock::now();
    rocksdb_scan_bench();
    end = std::chrono::high_resolution_clock::now();
    PRINT_TIME(rocksdb_scan_bench);
}

void db_bench() {
    for (size_t i = 0; i < repeat_times; ++i) {
        write_bench();
        read_bench();
        scan_bench();
    }
}

#endif