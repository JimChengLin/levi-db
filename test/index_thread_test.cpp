#include <iostream>
#include <random>

#include "../include/exception.h"
#include "../src/index_debug.h"

void index_thread_test() {
    const std::string fname = "/tmp/bdt";
    static constexpr int test_times = 1000;
    static constexpr int thread_num = 2;

    if (levidb8::env_io::fileExist(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    {
        levidb8::BitDegradeTreeDebug tree(fname);
        {
            std::vector<std::thread> jobs;
            for (size_t i = 0; i < thread_num; ++i) {
                jobs.emplace_back([&tree](size_t n) noexcept {
                    size_t start = n * test_times;
                    size_t limit = start + test_times;
                    for (size_t j = start; j < limit; ++j) {
                        if (j % levidb8::kPageSize == 0) {
                            continue;
                        }
                        auto val = static_cast<uint32_t>(j);
                        try {
                            tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
                            assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val == val);
                        } catch (const levidb8::Exception & e) {
                            std::cout << e.toString() << std::endl;
                            return;
                        } catch (const std::exception &) {
                            return;
                        }
                    }
                }, i);
            }
            for (auto & job:jobs) {
                job.join();
            }
            for (size_t i = 0; i < thread_num * test_times; ++i) {
                if (i % levidb8::kPageSize == 0) {
                    continue;
                }
                auto val = static_cast<uint32_t>(i);
                assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val == val);
            }
        }
        {
            std::vector<std::thread> jobs;
            for (size_t i = 0; i < thread_num; ++i) {
                jobs.emplace_back([&tree](size_t n) noexcept {
                    size_t start = n * test_times;
                    size_t limit = start + test_times;
                    for (size_t j = start; j < limit; ++j) {
                        if (j % levidb8::kPageSize == 0) {
                            continue;
                        }
                        auto val = static_cast<uint32_t>(j);
                        try {
                            tree.remove({reinterpret_cast<const char *>(&val), sizeof(val)}, {});
                            assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val != val);
                        } catch (const levidb8::Exception & e) {
                            std::cout << e.toString() << std::endl;
                            return;
                        } catch (const std::exception &) {
                            return;
                        }
                    }
                }, i);
            }
            for (auto & job:jobs) {
                job.join();
            }
            assert(tree.size() == 0);
        }
        {
            std::vector<std::thread> jobs;
            for (size_t i = 0; i < thread_num; ++i) {
                jobs.emplace_back([&tree](size_t n) noexcept {
                    auto start = static_cast<uint32_t>(n * test_times);
                    auto limit = start + test_times + test_times;

                    auto seed = std::random_device{}();
                    std::default_random_engine gen(seed);
                    auto val = std::uniform_int_distribution<uint32_t>(start, limit)(gen);
                    try {
                        tree.find({reinterpret_cast<const char *>(&val), sizeof(val)});
                        val = std::uniform_int_distribution<uint32_t>(start, limit)(gen);
                        tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
                        val = std::uniform_int_distribution<uint32_t>(start, limit)(gen);
                        tree.remove({reinterpret_cast<const char *>(&val), sizeof(val)}, {});
                    } catch (const levidb8::Exception & e) {
                        std::cout << e.toString() << std::endl;
                        return;
                    } catch (const std::exception &) {
                        return;
                    }
                }, i);
            }
            for (auto & job:jobs) {
                job.join();
            }
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}