#include <iostream>

#include "../src/log_reader.h"
#include "../src/log_writer.h"

void log_test() {
    const std::string fname = "/tmp/levi_log";
    if (LeviDB::IOEnv::fileExists(fname)) {
        LeviDB::IOEnv::deleteFile(fname);
    }

    LeviDB::AppendableFile f(fname);
    LeviDB::LogWriter writer(&f);
    const std::vector<uint8_t> bkv = LeviDB::LogWriter::makeRecord(std::string(UINT16_MAX, 'A'), "");
    writer.addRecord({bkv.data(), bkv.size()});

    LeviDB::RandomAccessFile rf(fname);
    size_t len = 0;
    auto iter = LeviDB::LogReader::makeRawIterator(&rf, 0);
    while (iter->valid()) {
        len += iter->item().size();
        iter->next();
    }
    assert(len == bkv.size() + 3/* last char meta */);

    auto kv_iter = LeviDB::LogReader::makeIterator(&rf, 0);
    kv_iter->seekToFirst();
    assert(kv_iter->valid());
    assert(kv_iter->key() == LeviDB::Slice(bkv.data() + 3/* varint k_len */, bkv.size() - 3));
    assert(kv_iter->value()[0] == false); // no del

    const std::vector<uint8_t> compress_bkvs = LeviDB::LogWriter::makeCompressRecord({{"A", "B"},
                                                                                      {"C", "D"},
                                                                                      {"E", "F"}});
    uint32_t pos = writer.calcWritePos();
    writer.addCompressRecord({compress_bkvs.data(), compress_bkvs.size()});

    auto compress_kv_iter = LeviDB::LogReader::makeIterator(&rf, pos);
    compress_kv_iter->seekToFirst();
    assert(compress_kv_iter->valid());

    assert(compress_kv_iter->key() == "A");
    assert(compress_kv_iter->value()[0] == 'B');
    compress_kv_iter->next();

    assert(compress_kv_iter->key() == "C");
    assert(compress_kv_iter->value()[0] == 'D');
    compress_kv_iter->next();

    assert(compress_kv_iter->key() == "E");
    assert(compress_kv_iter->value()[0] == 'F');
    compress_kv_iter->next();
    assert(!compress_kv_iter->valid());

    std::cout << __FUNCTION__ << std::endl;
}