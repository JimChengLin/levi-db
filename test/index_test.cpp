#include <iostream>
#include <random>
#include <unordered_set>

#include "../src/index.h"

void index_test() {
    const std::string fname = "/tmp/bdt";
    static constexpr int test_times = 1000;
    static constexpr int test_times_plus = 10000;

    if (levidb8::env_io::fileExists(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    {
        levidb8::BitDegradeTree tree(fname);
        for (int i = 2; i < test_times; i += 2) {
            auto val = static_cast<uint32_t>(i);
            tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
            for (int j = 2; j <= i; j += 2) {
                val = static_cast<uint32_t>(j);
                assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val == val);
            }
        }
        assert(tree.size() == test_times / 2 - 1);

        for (int i = 2; i < test_times; i += 2) {
            auto val = static_cast<uint32_t>(i);
            tree.remove({reinterpret_cast<const char *>(&val), sizeof(val)}, {});
            assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val != val);
            for (int j = i + 2; j < test_times; j += 2) {
                val = static_cast<uint32_t>(j);
                assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val == val);
            }
        }
        assert(tree.size() == 0);

        for (int i = 2; i < test_times; i += 2) {
            auto val = static_cast<uint32_t>(i);
            tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
            for (int j = 2; j <= i; j += 2) {
                val = static_cast<uint32_t>(j);
                assert(tree.find({reinterpret_cast<const char *>(&val), sizeof(val)}).val == val);
            }
        }
    }

    auto seed = std::random_device{}();
    {
        levidb8::env_io::deleteFile(fname);
        levidb8::BitDegradeTree tree(fname);
        std::unordered_set<std::string> ctrl;
        std::default_random_engine gen(seed);

        for (int i = 0; i < test_times_plus; ++i) {
            auto val = std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(gen);
            if (val == levidb8::kDiskNull || val % levidb8::kPageSize == 0) {
                continue;
            }

            tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
            ctrl.emplace(reinterpret_cast<const char *>(&val), reinterpret_cast<const char *>(&val) + sizeof(val));

            if (std::uniform_int_distribution<uint32_t>(0, 1)(gen) == 0) {
                tree.remove(*ctrl.cbegin(), {});
                ctrl.erase(ctrl.begin());
            }
        }

        for (const auto & k:ctrl) {
            uint32_t val;
            memcpy(&val, k.data(), sizeof(val));
            assert(tree.find(k).val == val);
        }
        for (const auto & k:ctrl) {
            tree.remove(k, {});
        }
        assert(tree.size() == 0);
    }

    std::cout << __FUNCTION__ << " - " << seed << std::endl;
}