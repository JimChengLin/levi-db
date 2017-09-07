#include <iostream>

#include "../src/index_mvcc_rd.h"

void index_mvcc_test() {
    const std::string fname = "/tmp/bdt_mvcc";
    static constexpr int test_times_ = 10;

    if (LeviDB::IOEnv::fileExists(fname)) {
        LeviDB::IOEnv::deleteFile(fname);
    }

    LeviDB::SeqGenerator seq_g;
    LeviDB::IndexMVCC index(fname, &seq_g);

    auto verify = [](const LeviDB::IndexMVCC & e_group, uint64_t seq_num,
                     const std::vector<uint32_t> & c_group) {
        for (int i = 0, idx = 0; i <= c_group.back(); i += 2) {
            auto val = static_cast<uint32_t>(i);
            if (val == c_group[idx]) {
                ++idx;
                if (e_group.find({reinterpret_cast<char *>(&val), sizeof(val)}, seq_num).val != val)
                    return false;
            } else {
                if (e_group.find({reinterpret_cast<char *>(&val), sizeof(val)}, seq_num).val == val)
                    return false;
            }
        }
        return true;
    };

    uint32_t init = 0;
    std::vector<uint32_t> ctrl_group;
    for (int i = 0; i < test_times_; ++i) {
        uint32_t val = (init += 2);
        index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
        ctrl_group.emplace_back(val);
    }

    {
        std::vector<uint32_t> ctrl_group2 = ctrl_group;
        for (int i = 0; i < test_times_; ++i) {
            uint32_t val = (init += 2);
            index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            ctrl_group2.emplace_back(val);
        }
        auto s2 = seq_g.makeSnapshot();

        std::vector<uint32_t> ctrl_group3 = ctrl_group2;
        uint32_t v = ctrl_group3.back();
        index.remove({reinterpret_cast<char *>(&v), sizeof(v)});
        ctrl_group3.pop_back();
        for (int i = 0; i < test_times_; ++i) {
            uint32_t val = (init += 2);
            index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            ctrl_group3.emplace_back(val);
        }
        auto s3 = seq_g.makeSnapshot();

        ctrl_group = ctrl_group3;
        for (int i = 0; i < test_times_; ++i) {
            uint32_t val = (init += 2);
            index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            ctrl_group.emplace_back(val);
        }

        assert(verify(index, s2->immut_seq_num(), ctrl_group2));
        assert(verify(index, s3->immut_seq_num(), ctrl_group3));
        assert(verify(index, 0, ctrl_group));
        assert(!index.sync());
    }
    {
        auto guard = seq_g.makeSnapshot();
        index.tryApplyPending();
    }
    uint32_t val = ctrl_group.back();
    ctrl_group.pop_back();
    index.remove({reinterpret_cast<char *>(&val), sizeof(val)});

    bool res = index.sync();
    assert(res);
    assert(verify(index, 0, ctrl_group));

    std::cout << __FUNCTION__ << std::endl;
}