#include <map>
#include <vector>

#include "db.h"
#include "index_read.h"
#include "keeper.h"
#include "log_reader.h"
#include "log_writer.h"
#include "optional.h"

namespace levidb8 {
    struct DBMeta {
        OffsetToEmpty _offset{kDiskNull};
    };

    class DBImpl : public DB {
    private:
        Optional<AppendableFile> _af;
        Optional<RandomAccessFile> _rf;
        Optional<LogWriter> _writer;
        Optional<BitDegradeTreeReadLog> _index;
        Optional<WeakKeeper<DBMeta>> _meta;

    public:
        DBImpl(const std::string & name, const OpenOptions & options) {
            std::string prefix = name + '/';

            if (env_io::fileExists(name)) {
                if (options.error_if_exists) {
                    throw Exception::invalidArgumentException("DB already exist");
                }
                // 打开现有数据库
                std::string data_fname = prefix + "data";
                if (!env_io::fileExists(data_fname)) {
                    throw Exception::notFoundException("data file missing", data_fname);
                }
                std::string index_fname = prefix + "index";
                std::string keeper_fname = std::move(prefix += "keeper");
                if (!env_io::fileExists(index_fname) || !env_io::fileExists(keeper_fname)) { // repair
                    if (env_io::fileExists(index_fname)) {
                        env_io::deleteFile(index_fname);
                    }
                    if (env_io::fileExists(keeper_fname)) {
                        env_io::deleteFile(keeper_fname);
                    }
                    _meta.build(std::move(keeper_fname), DBMeta{}, std::string());
                    _af.build(data_fname);
                    _rf.build(std::move(data_fname));
                    _index.build(std::move(index_fname), _rf.get());
                    _writer.build(_af.get(), _af->immut_length());

                    auto it = log_reader::makeTableIterator(_rf.get());
                    it->prepare();
                    while (true) {
                        it->next();
                        if (!it->valid()) {
                            break;
                        }
                        auto item = it->item();
                        _index->insert(item.first, OffsetToData{item.second.first});
                    }
                } else {
                    _meta.build(std::move(keeper_fname));
                    _af.build(data_fname);
                    _rf.build(std::move(data_fname));
                    _index.build(std::move(index_fname), _meta->immut_value()._offset, _rf.get());
                    _writer.build(_af.get(), _af->immut_length());
                }
            } else {
                if (!options.create_if_missing) {
                    throw Exception::notFoundException("DB not found");
                }
                // 新建数据库
                env_io::createDir(name);
                std::string data_fname = prefix + "data";
                _af.build(data_fname);
                _rf.build(std::move(data_fname));
                _index.build(prefix + "index", _rf.get());
                _writer.build(_af.get());
                _meta.build(std::move(prefix += "keeper"), DBMeta{}, std::string());
            }
        }

        ~DBImpl() noexcept override {
            _meta->mut_value()._offset = _index->immut_empty();
        };

        bool put(const Slice & key,
                 const Slice & value,
                 const PutOptions & options) override {
            try {
                auto bkv = LogWriter::makeRecord(key, value);
                uint32_t pos = _writer->addRecord({bkv.data(), bkv.size()});
                _index->insert(key, OffsetToData{pos});

                if (options.sync) {
                    sync();
                }
                return true;
            } catch (const IndexFullControlledException &) {
                return false;
            } catch (const LogFullControlledException &) {
                return false;
            }
        }

        bool remove(const Slice & key,
                    const RemoveOptions & options) override {
            try {
                auto bkv = LogWriter::makeRecord(key, {});
                uint32_t pos = _writer->addRecordForDel({bkv.data(), bkv.size()});
                _index->remove(key, OffsetToData{pos});

                if (options.sync) {
                    sync();
                }
                return true;
            } catch (const IndexFullControlledException &) {
                return false;
            } catch (const LogFullControlledException &) {
                return false;
            }
        }

        bool write(const std::vector<std::pair<Slice, Slice>> & kvs,
                   const WriteOptions & options) override {
            assert(!kvs.empty());
            try {
                bool compress = options.try_compress;
                size_t uncompress_size = 0;
                for (const auto & kv:kvs) {
                    if (kv.second.data() == nullptr) {
                        compress = false;
                        break;
                    }
                    uncompress_size += kv.first.size() + kv.second.size();
                }

                if (compress) {
                    auto bkvs = LogWriter::makeCompressedRecords(kvs);
                    if (bkvs.size() <= uncompress_size - uncompress_size / 8) {
                        uint32_t pos = _writer->addCompressedRecords({bkvs.data(), bkvs.size()});
                        for (const auto & kv:kvs) {
                            _index->insert(kv.first, OffsetToData{pos});
                        }
                        return true;
                    }
                }

                std::vector<std::vector<uint8_t>> bkvs;
                std::vector<Slice> slice_bkvs;
                std::vector<uint32_t> addrs;
                bkvs.reserve(kvs.size());
                slice_bkvs.reserve(kvs.size());
                addrs.reserve(kvs.size());

                for (const auto & kv:kvs) {
                    bkvs.emplace_back(LogWriter::makeRecord(kv.first, kv.second));
                    slice_bkvs.emplace_back(bkvs.back().data(), bkvs.back().size());
                    addrs.emplace_back(kv.second.data() == nullptr); // 1 = del
                }

                addrs = _writer->addRecordsMayDel(slice_bkvs, std::move(addrs));
                assert(addrs.size() == kvs.size());
                for (size_t i = 0; i < kvs.size(); ++i) {
                    if (kvs[i].second.data() != nullptr) {
                        _index->insert(kvs[i].first, OffsetToData{addrs[i]});
                    } else {
                        _index->remove(kvs[i].first, OffsetToData{addrs[i]});
                    }
                }

                if (options.sync) {
                    sync();
                }
                return true;
            } catch (const IndexFullControlledException &) {
                return false;
            } catch (const LogFullControlledException &) {
                return false;
            }
        }

        std::pair<std::string, bool>
        get(const Slice & key,
            const ReadOptions & options) const override {
            return _index->find(key);
        };

        std::unique_ptr<Iterator<Slice, Slice>>
        scan(const ScanOptions & options) const override {
            return _index->scan();
        };

        void sync() override {
            _af->sync();
            _index->sync();
        }
    };

    std::unique_ptr<DB> DB::open(const std::string & name, const OpenOptions & options) {
        return std::make_unique<DBImpl>(name, options);
    }

    bool repairDB(const std::string & name,
                  std::function<void(const Exception &, uint32_t)> reporter) noexcept {
        try {
            {
                RandomAccessFile rf(name + "/data");
                auto it = log_reader::makeRecoveryIterator(&rf, reporter);
                auto temp_db = DB::open(name + "_tmp", {true, true});
                std::map<std::string, std::string, SliceComparator> q;
                std::vector<std::pair<Slice, Slice>> slice_q;
                size_t uncompress_size = 0;

                it->prepare();
                while (true) {
                    it->next();
                    if (!it->valid()) {
                        break;
                    }

                    auto item = it->item();
                    if (item.second.second.del) {
                        temp_db->remove(item.first, {});
                        auto find_res = q.find(item.first);
                        if (find_res != q.end()) {
                            q.erase(find_res);
                            uncompress_size -= (item.first.size() + item.second.first.size());
                        }
                    } else {
                        q[item.first.toString()] = item.second.first.toString();
                        uncompress_size += (item.first.size() + item.second.first.size());
                    }

                    if (uncompress_size >= kPageSize) {
                        slice_q.reserve(q.size());
                        for (const auto & kv:q) {
                            slice_q.emplace_back(kv.first, kv.second);
                        }
                        temp_db->write(slice_q, {});

                        q.clear();
                        slice_q.clear();
                        uncompress_size = 0;
                    }
                }
                if (uncompress_size != 0) {
                    slice_q.reserve(q.size());
                    for (const auto & kv:q) {
                        slice_q.emplace_back(kv.first, kv.second);
                    }
                    temp_db->write(slice_q, {});

                    q.clear();
                    slice_q.clear();
                }
            }
            destroyDB(name);
            env_io::renameFile(name + "_tmp", name);
        } catch (const Exception & e) {
            reporter(e, UINT32_MAX);
            return false;
        } catch (const std::exception &) {
            return false;
        }
        return true;
    }

    void destroyDB(const std::string & name) {
        for (const std::string & child:env_io::getChildren(name)) {
            env_io::deleteFile((name + '/') += child);
        }
        env_io::deleteDir(name);
    }
}