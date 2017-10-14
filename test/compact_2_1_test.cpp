#include <iostream>

#include "../src/aggregator/compact_2_1.h"

void compact_2_1_test() {
    for (const std::string & name:{"/tmp/lv_db_a+lv_db_b", "/tmp/lv_db_a+lv_db_b-"}) {
        if (LeviDB::IOEnv::fileExists(name)) {
            for (const std::string & child:LeviDB::IOEnv::getChildren(name)) {
                LeviDB::IOEnv::deleteFile((name + '/') += child);
            }
            LeviDB::IOEnv::deleteDir(name);
        }
    }

    const std::string db_name = "/tmp/lv_db";
    LeviDB::SeqGenerator seq_gen;
    LeviDB::Options opt{};
    opt.compression = false;
    LeviDB::Compacting2To1Worker worker(std::make_unique<LeviDB::DBSingle>(db_name + "_a", opt, &seq_gen),
                                        std::make_unique<LeviDB::DBSingle>(db_name + "_b", LeviDB::Options{}, &seq_gen),
                                        &seq_gen);

    const auto & product = worker.immut_product();
    auto it = product->makeIterator(product->makeSnapshot());
    it->seekToFirst();
    std::string prev_val;
    while (it->valid()) {
        assert(LeviDB::SliceComparator{}(prev_val, it->key()));
        assert(it->key() == it->value());
        prev_val = it->key().toString();
        it->next();
    }
    assert(prev_val == "99");

    std::cout << __FUNCTION__ << std::endl;
}