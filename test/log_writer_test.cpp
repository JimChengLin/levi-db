#include "../src/log_writer.h"
#include <iostream>

void log_writer_test() noexcept {
    const std::string fname = "/tmp/levidb_test";

    if (LeviDB::IOEnv::fileExists(fname)) {
        LeviDB::IOEnv::deleteFile(fname);
    }

    {
        const std::string sample("123456789");
        LeviDB::AppendableFile file(fname);
        LeviDB::LogWriter writer(&file);

        writer.addRecord(LeviDB::LogWriter::Record{sample, {}, false});
        writer.addRecord(LeviDB::LogWriter::Record{sample, {}, true});
        writer.addRecord(LeviDB::LogWriter::Record{std::string(UINT8_MAX + 10, 'A') + std::string(10, 'B'), {}, false});
    }

    LeviDB::IOEnv::deleteFile(fname);
    std::cout << __FUNCTION__ << std::endl;
}