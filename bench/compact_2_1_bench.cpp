#ifdef LEVI_BENCH

#include <iostream>

#include "../src/aggregator/compact_2_1.h"

void compact_2_1_bench() {
    for (const std::string & name:{"/tmp/lv_bench_db_a+lv_bench_db_b", "/tmp/lv_bench_db_a+lv_bench_db_b-"}) {
        if (LeviDB::IOEnv::fileExists(name)) {
            for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                LeviDB::IOEnv::deleteFile((name + '/') += child);
            }
            LeviDB::IOEnv::deleteDir(name);
        }
    }

    static constexpr int test_times_ = 10000;
    const std::string db_name = "/tmp/lv_bench_db";
    LeviDB::SeqGenerator seq_gen;
    LeviDB::Compacting2To1Worker worker(
            std::make_unique<LeviDB::DBSingle>(db_name + "_a", LeviDB::Options{}, &seq_gen),
            std::make_unique<LeviDB::DBSingle>(db_name + "_b", LeviDB::Options{}, &seq_gen),
            &seq_gen);

    int i = 0;
    auto it = worker.immut_product()->makeIterator(worker.mut_product()->makeSnapshot());
    it->seekToFirst();
    while (it->valid()) {
        ++i;
        assert(it->key().size() != 0 && !it->value().empty());
        it->next();
    }
    assert(i == test_times_ * 2);

    std::cout << __FUNCTION__ << std::endl;
}

#endif // LEVI_BENCH