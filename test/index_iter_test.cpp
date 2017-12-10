#include <iostream>

#include "../src/index_debug.h"

void index_iter_test() {
    const std::string fname = "/tmp/bdt";
    static constexpr int test_times = 1000;

    if (levidb8::env_io::fileExist(fname)) {
        levidb8::env_io::deleteFile(fname);
    }

    {
        levidb8::BitDegradeTreeDebug tree(fname);
        for (size_t i = 2; i < test_times; i += 2) {
            auto val = static_cast<uint32_t>(i);
            tree.insert({reinterpret_cast<const char *>(&val), sizeof(val)}, {val});
        }

        levidb8::BitDegradeTreeDebug::BDIterator iter(&tree);
        uint32_t prev_val = 0;
        uint32_t cnt = 0;
        for (iter.seekToFirst();
             iter.valid();
             iter.next()) {
            uint32_t val = iter.value().val;
            assert(memcmp(&prev_val, &val, sizeof(val)) < 0);
            prev_val = val;
            ++cnt;
        }
        assert(cnt == test_times / 2 - 1);

        prev_val = UINT32_MAX;
        cnt = 0;
        for (iter.seekToLast();
             iter.valid();
             iter.prev()) {
            uint32_t val = iter.value().val;
            assert(memcmp(&prev_val, &val, sizeof(val)) > 0);
            prev_val = val;
            ++cnt;
        }
        assert(cnt == test_times / 2 - 1);
    }

    std::cout << __FUNCTION__ << std::endl;
}