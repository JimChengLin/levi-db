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
    assert(regex_check(R("a") << "bcda", "abcda"));

    insert_k("bbcd");
    assert(regex_check(R("b", 1, 2) << "cd", "bbcd"));

    insert_k("abcccc");
    assert(regex_check(R("ab") << R("c", 0, INT_MAX), "abcccc"));

    insert_k("abc");
    assert(regex_check(R("ab") << R("c", 0, INT_MAX, LeviDB::Regex::LAZY), "abc"));

    insert_k("k123k");
    assert(regex_check(R("k") << R(std::string(1, LeviDB::uint8ToChar(0)),
                                   std::string(1, LeviDB::uint8ToChar(UINT8_MAX)), 0, INT_MAX) << "k", "k123k"));

    insert_k("abcd");
    assert(regex_check((R("abc") & R("abc")) << "d", "abcd"));
    assert(regex_check((R("cba") | R("abc")) << "d", "abcd"));
    assert(regex_check(R(std::make_unique<R>(~R("d")), 3, 3), "abc"));

    std::cout << __FUNCTION__ << std::endl;
}