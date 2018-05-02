#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>

#if defined(LEVIDB_BENCH)
#include <rocksdb/db.h>
#endif

#include "env.h"

#include "../include/db.h"

namespace levidb::db_bench {
    class ManifestorImpl : public Manifestor {
    private:
        std::map<std::string, std::string, SliceComparator> map_;

    public:
        void Set(const Slice & k, const Slice & v) override {
            map_.emplace(k.ToString(), v.ToString());
        }

        bool Get(const Slice & k, std::string * v) const override {
            auto it = map_.find(k);
            if (it != map_.cend()) {
                v->assign(it->second);
                return true;
            }
            return false;
        }
    };

    class TextProvider {
    private:
        std::ifstream f_;
        std::string k_;
        std::string v_;
        std::string line_;
        size_t cnt_;

    public:
        explicit TextProvider(std::ifstream && f)
                : f_(std::move(f)),
                  cnt_(0) {}

        std::pair<Slice, Slice>
        ReadItem() {
            k_.clear();
            v_.clear();
            for (size_t i = 0; i < 2; ++i) {
                std::getline(f_, line_);
                k_.append(line_);
            }
            if (k_.size() > 1000) {
                k_.resize(1000);
            }
            for (auto & c: k_) {
                if (c == '\0') {
                    ++c;
                }
            }
            k_.append(std::to_string(cnt_++));
            for (size_t i = 0; i < 7; ++i) {
                std::getline(f_, line_);
                v_.append(line_);
            }
            return {{k_.c_str(), k_.size() + 1}, v_};
        };

        void SkipItem() {
            for (size_t i = 0; i < 9; ++i) {
                std::getline(f_, line_);
            }
            ++cnt_;
        }
    };

#define TIME_START auto start = std::chrono::high_resolution_clock::now()
#define TIME_END auto end = std::chrono::high_resolution_clock::now()
#define PRINT_TIME(name) \
std::cout << #name " took " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl

    void Run() {
        constexpr char kPathText[] = "/Users/yuanjinlin/Desktop/movies.txt";

        constexpr char kPathDB[] = "/tmp/levi-db";
        constexpr char kPathRocksDB[] = "/tmp/rocks-db";
        constexpr unsigned int kTestTimes = 1000000;
        constexpr unsigned int kThreadNum = 8;

        auto * env = penv::Env::Default();
        if (!env->FileExists(kPathText)) {
            return;
        }
        if (env->FileExists(kPathDB)) {
            env->DeleteAll(kPathDB);
        }
        if (env->FileExists(kPathRocksDB)) {
            env->DeleteAll(kPathRocksDB);
        }

        {
            ManifestorImpl manifestor;
            auto db = DB::Open(kPathDB, OpenOptions{&manifestor});
            {
                TIME_START;
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        TextProvider provider(std::ifstream{kPathText});
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, v] = provider.ReadItem();
                                db->Add(k, v);
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
                TIME_END;
                PRINT_TIME(levidb - Add);
            }
            {
                TIME_START;
                std::atomic<size_t> total(0);
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        std::string buf;
                        TextProvider provider(std::ifstream{kPathText});
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, v] = provider.ReadItem();
                                db->Get(k, &buf);
                                assert(v == buf);
                                total += buf.size();
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
                TIME_END;
                PRINT_TIME(levidb - Get);
                std::cout << "levidb_get_size: " << total << std::endl;
            }
            {
                TIME_START;
                size_t total = 0;
                auto iter = db->GetIterator();
                for (iter->SeekToFirst();
                     iter->Valid();
                     iter->Next()) {
                    total += iter->Key().size() + iter->Value().size();
                }
                TIME_END;
                PRINT_TIME(levidb - Iterate);
                std::cout << "levidb_iterate_size: " << total << std::endl;
            }
        }
#if defined(LEVIDB_BENCH)
        {
            rocksdb::DB * db;
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::DB::Open(options, kPathRocksDB, &db);
            {
                TIME_START;
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        TextProvider provider(std::ifstream{kPathText});
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, v] = provider.ReadItem();
                                db->Put({}, {k.data(), k.size()}, {v.data(), v.size()});
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
                TIME_END;
                PRINT_TIME(rocksdb - Put);
            }
            {
                TIME_START;
                std::atomic<size_t> total(0);
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        std::string buf;
                        TextProvider provider(std::ifstream{kPathText});
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, _] = provider.ReadItem();
                                db->Get({}, {k.data(), k.size()}, &buf);
                                total += buf.size();
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
                TIME_END;
                PRINT_TIME(rocksdb - Get);
                std::cout << "rocksdb_get_size: " << total << std::endl;
            }
            {
                TIME_START;
                size_t total = 0;
                auto * iter = db->NewIterator({});
                for (iter->SeekToFirst();
                     iter->Valid();
                     iter->Next()) {
                    total += iter->key().size() + iter->value().size();
                }
                delete iter;
                TIME_END;
                PRINT_TIME(rocksdb - Iterate);
                std::cout << "rocksdb_iterate_size: " << total << std::endl;
            }
            delete db;
        }
#endif
    }
}