#include <iostream>

#include "../src/index_iter_regex.h"
#include "../src/log_writer.h"

void index_regex_test() {
    typedef LeviDB::Regex::R R;

    const std::string index_fname = "/tmp/bdt_regex_index";
    const std::string data_fname = "/tmp/bdt_regex_data";

    if (LeviDB::IOEnv::fileExists(index_fname)) {
        LeviDB::IOEnv::deleteFile(index_fname);
    }
    if (LeviDB::IOEnv::fileExists(data_fname)) {
        LeviDB::IOEnv::deleteFile(data_fname);
    }

    LeviDB::AppendableFile af(data_fname);
    LeviDB::RandomAccessFile rf(data_fname);

    LeviDB::SeqGenerator seq_g;
    LeviDB::IndexRegex index(index_fname, &seq_g, &rf);
    LeviDB::LogWriter writer(&af);

    auto insert_k = [&](std::string k) {
        uint32_t pos = writer.calcWritePos();
        std::vector<uint8_t> bin = LeviDB::LogWriter::makeRecord(k, {});
        writer.addRecord({bin.data(), bin.size()});
        index.insert(k, LeviDB::OffsetToData{pos});
    };

    auto regex_check = [&](R r, std::string expect) {
        auto regex_iter = index.makeRegexIterator(std::make_shared<R>(std::move(r)));
        return regex_iter->valid() && regex_iter->item().first == expect;
    };

    insert_k("abcda");
    assert(regex_check(R("abcda"), "abcda"));

    std::cout << __FUNCTION__ << std::endl;
}