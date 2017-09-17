#include <iostream>

#include "../src/exception.h"
#include "../src/index.h"

void misc_test() {
    { // empty node checksum
        const std::string fname = "/tmp/misc_bdt";
        if (LeviDB::IOEnv::fileExists(fname)) {
            LeviDB::IOEnv::deleteFile(fname);
        }
        {
            LeviDB::BitDegradeTree bdt(fname);
            for (int i = 0; i < LeviDB::IndexConst::rank_ * 2 + 3; i += 2) {
                auto val = static_cast<uint32_t>(i);
                bdt.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            }
            for (int i = 0; i < LeviDB::IndexConst::rank_ * 2 + 3; i += 2) {
                auto val = static_cast<uint32_t>(i);
                bdt.remove({reinterpret_cast<char *>(&val), sizeof(val)});
            }
        }
        {
            LeviDB::RandomWriteFile rwf(fname);
            rwf.write(4097, "6");
        }
        LeviDB::BitDegradeTree bdt(fname, LeviDB::OffsetToEmpty{4096});
        try {
            for (int i = 0; i < LeviDB::IndexConst::rank_ * 2 + 3; i += 2) {
                auto val = static_cast<uint32_t>(i);
                bdt.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            }
            assert(false);
        } catch (const LeviDB::Exception & e) {
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}