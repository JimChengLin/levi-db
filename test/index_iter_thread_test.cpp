#include <array>
#include <iostream>
#include <unordered_set>

#include "../include/exception.h"
#include "../src/index_debug.h"

void index_iter_thread_test() {
    const std::string fname = "/tmp/bdt";
    static constexpr int test_times = 1000;
    static constexpr int thread_num = 2;

    if (levidb8::env_io::fileExist(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    {
        std::array<std::atomic<int>, thread_num * test_times> done_arr{};
        std::atomic<int> seq_num{0};

        levidb8::BitDegradeTreeDebug tree(fname);
        std::vector<std::thread> jobs;
        for (size_t i = 0; i < thread_num; ++i) {
            jobs.emplace_back([&tree, &done_arr, &seq_num](size_t n) noexcept {
                size_t start = n * test_times;
                size_t limit = start + test_times;
                for (size_t j = start; j < limit; ++j) {
                    if (j % levidb8::kPageSize == 0) {
                        continue;
                    }
                    auto val = static_cast<uint32_t>(j);
                    try {
                        tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
                        done_arr[j].store(seq_num.fetch_add(1), std::memory_order_release);
                    } catch (const levidb8::Exception & e) {
                        std::cout << e.toString() << std::endl;
                        return;
                    } catch (const std::exception &) {
                        return;
                    }
                }
            }, i);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        int done_time = seq_num.fetch_add(1);

        std::unordered_set<uint32_t> exist;
        levidb8::BitDegradeTreeDebug::BDIterator iter(&tree);
        for (iter.seekToFirst();
             iter.valid();
             iter.next()) {
            exist.emplace(iter.value().val);
        }

        std::vector<uint32_t> expect;
        for (size_t i = 0; i < done_arr.size(); ++i) {
            if (i % levidb8::kPageSize == 0) {
                continue;
            }
            int v = done_arr[i].load(std::memory_order_acquire);
            if (v == 0 || v > done_time) {
                continue;
            }
            expect.emplace_back(i);
        }

        for (uint32_t val:expect) {
            assert(exist.find(val) != exist.cend());
        }
        for (auto & job:jobs) {
            job.join();
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}
