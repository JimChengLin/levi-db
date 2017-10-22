#ifdef LEVI_BENCH

#include <iostream>

#include "../src/index_iter_regex.h"

void kv_iter_bench() {
    const std::string index_fname = "/tmp/levi_bench_index";
    const std::string data_fname = "/tmp/levi_bench_data";
    if (!LeviDB::IOEnv::fileExists(index_fname) || !LeviDB::IOEnv::fileExists(data_fname)) {
        return;
    }

    LeviDB::SeqGenerator seq_g;
    LeviDB::RandomAccessFile rf(data_fname);
    const LeviDB::IndexIter bdt(index_fname, LeviDB::OffsetToEmpty{LeviDB::IndexConst::disk_null_}, &seq_g, &rf);

    auto it = bdt.makeIterator();
    it->seekToFirst();
    while (it->valid()) {
        assert(it->key().size() != 0 && !it->value().empty());
        it->next();
    }

    it->seekToLast();
    std::string target = it->key().toString();
    while (it->valid()) {
        assert(it->key().size() != 0 && !it->value().empty());
        it->prev();
    }

    it->seek(target);
    while (it->valid()) {
        assert(it->key().size() != 0 && !it->value().empty());
        it->prev();
    }

    std::cout << __FUNCTION__ << std::endl;
}

#endif // LEVI_BENCH