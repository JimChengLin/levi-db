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
    std::vector<uint8_t> bkv = LeviDB::LogWriter::makeRecord(std::string(UINT16_MAX, 'A'), "");
    writer.addRecord({bkv.data(), bkv.size()});

    LeviDB::RandomAccessFile rf(fname);
    size_t length = 0;
    auto iter = LeviDB::LogReader::makeRawIterator(&rf, 0);
    while (iter->valid()) {
        length += iter->item().size();
        iter->next();
    }
    assert(length == bkv.size() + 3/* last char meta */);

    auto kv_iter = LeviDB::LogReader::makeIterator(&rf, 0);
    kv_iter->seekToFirst();
    assert(kv_iter->valid());
    assert(kv_iter->key() == LeviDB::Slice(bkv.data() + 3/* varint k_len */, bkv.size() - 3));
    assert(kv_iter->value()[0] == false); // no del
    kv_iter->next();
    assert(!kv_iter->valid());

    std::vector<uint8_t> compress_bkvs = LeviDB::LogWriter::makeCompressRecord({{"A", "B"},
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

    compress_kv_iter->seekToLast();
    assert(compress_kv_iter->key() == "E");
    compress_kv_iter->seek("C");
    assert(compress_kv_iter->key() == "C");
    compress_kv_iter->prev();
    assert(compress_kv_iter->key() == "A");

    pos = writer.calcWritePos();
    std::string value_input(UINT16_MAX, 'B');
    bkv = LeviDB::LogWriter::makeRecord("KEY", value_input);
    writer.addRecord({bkv.data(), bkv.size()});

    kv_iter = LeviDB::LogReader::makeIterator(&rf, pos);
    kv_iter->seekToFirst();
    assert(kv_iter->valid());
    assert(kv_iter->key() == "KEY");
    std::string value = kv_iter->value();
    value.pop_back();
    assert(value == value_input);
    kv_iter->prev();
    assert(!kv_iter->valid());

    length = 0;
    auto table_iter = LeviDB::LogReader::makeTableIterator(&rf);
    while (table_iter->valid()) {
        length += table_iter->item().first.size() + table_iter->item().second.size();
        table_iter->next();
    }
    assert(length == UINT16_MAX + 1 + 3 + 3 + 3 + UINT16_MAX + 3 + 1);

    std::cout << __FUNCTION__ << std::endl;
}