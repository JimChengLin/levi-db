#include <iostream>

#include "../src/index.h"

void index_test() {
    const std::string fname = "/tmp/bdt";
    constexpr int test_times_ = 1000;

    if (LeviDB::IOEnv::fileExists(fname)) {
        LeviDB::IOEnv::deleteFile(fname);
    }

    LeviDB::BitDegradeTree tree(fname);
    for (int i = 0; i < test_times_; i += 2) {
        auto val = static_cast<uint32_t>(i);
        tree.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
        for (int j = 0; j <= i; j += 2) {
            val = static_cast<uint32_t>(j);
            assert(tree.find({reinterpret_cast<char *>(&val), sizeof(val)}).val == val);
        }
    }

    for (int i = 0; i < test_times_; i += 2) {
        auto val = static_cast<uint32_t>(i);
        tree.remove({reinterpret_cast<char *>(&val), sizeof(val)});
        assert(tree.find({reinterpret_cast<char *>(&val), sizeof(val)}).val != val);
        for (int j = i + 2; j < test_times_; j += 2) {
            val = static_cast<uint32_t>(j);
            assert(tree.find({reinterpret_cast<char *>(&val), sizeof(val)}).val == val);
        }
    }
    assert(tree.size() == 0);

    for (int i = 0; i < test_times_; i += 2) {
        auto val = static_cast<uint32_t>(i);
        tree.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
        for (int j = 0; j <= i; j += 2) {
            val = static_cast<uint32_t>(j);
            assert(tree.find({reinterpret_cast<char *>(&val), sizeof(val)}).val == val);
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}