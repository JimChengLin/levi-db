#include <iostream>

#include "../src/exception.h"
#include "../src/index_mvcc_rd.h"
#include "../src/log_reader.h"
#include "../src/log_writer.h"

void misc_test() {
    const std::string fname = "/tmp/misc_bdt";
    if (LeviDB::IOEnv::fileExists(fname)) {
        LeviDB::IOEnv::deleteFile(fname);
    }
    { // empty node checksum
        {
            LeviDB::BitDegradeTree bdt(fname);
            for (int i = 0; i < LeviDB::IndexConst::rank_ * 2 + 3; i += 2) {
                auto val = static_cast<uint32_t>(i);
                bdt.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            }
            for (int i = 0; i < LeviDB::IndexConst::rank_ * 2 + 3; i += 2) {
                auto val = static_cast<uint32_t>(i);
                bdt.remove({reinterpret_cast<char *>(&val), sizeof(val)}, {});
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
    { // ob mode usr
        std::string model = "ABC";
        LeviDB::USR u(&model);
        LeviDB::USR u2(model);
        assert(u.toSlice() == u2.toSlice());
    }
    { // pending part
        LeviDB::IOEnv::deleteFile(fname);
        LeviDB::SeqGenerator seq_g;
        LeviDB::IndexMVCC index(fname, &seq_g);

        std::vector<std::unique_ptr<LeviDB::Snapshot>> snapshots;
        for (int i = 0; i < 20; i += 2) {
            auto val = static_cast<uint32_t>(i);
            index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            val += 2;
            index.insert({reinterpret_cast<char *>(&val), sizeof(val)}, {val});
            snapshots.emplace_back(seq_g.makeSnapshot());
        }

        auto pending_it = index.pendingPart(seq_g.newest());
        pending_it->seekToFirst();
        for (int i = 0; i < 9; ++i) {
            pending_it->next();
        }
        for (int i = 0; i < 9; ++i) {
            pending_it->prev();
        }
        for (int i = 0; i < 9; ++i) {
            pending_it->next();
        }
        assert(*reinterpret_cast<const uint32_t *>(pending_it->key().data()) == 12);
        snapshots.clear();
        index.tryApplyPending();
    }
    { // log trailing
        LeviDB::IOEnv::deleteFile(fname);
        LeviDB::AppendableFile af(fname);
        LeviDB::LogWriter writer(&af);
        std::vector<uint8_t> bkv = LeviDB::LogWriter::makeRecord(std::string(32752, 'A'), "");
        for (int i = 0; i < 2; ++i) {
            writer.addRecord({bkv.data(), bkv.size()});
        }

        LeviDB::RandomAccessFile rf(fname);
        auto iter = LeviDB::LogReader::makeRawIterator(&rf, 0);
        while (iter->valid()) {
            assert(iter->item().size() == 32752 + 3 + 1);
            iter->next();
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}