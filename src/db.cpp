#include <vector>
#include <map>

#include "../include/db.h"
#include "index_read.h"
#include "keeper.h"
#include "log_writer.h"
#include "optional.h"

namespace levidb8 {
    struct DBMeta {
        OffsetToEmpty offset{kDiskNull};
    };

    class DBImpl : public DB {
    private:
        Optional<AppendableFile> _af;
        Optional<RandomAccessFile> _rf;
        Optional<LogWriter> _writer;
        Optional<BitDegradeTreeRead> _index;
        Optional<WeakKeeper<DBMeta>> _meta;

    public:
        DBImpl(const std::string & name, const OpenOptions & options) {
            std::string prefix = name + '/';

            if (env_io::fileExist(name)) {
                if (options.error_if_exist) {
                    throw Exception::invalidArgumentException("DB already exist");
                }
                // 打开现有数据库
                std::string data_fname = prefix + "data";
                if (!env_io::fileExist(data_fname)) {
                    throw Exception::notFoundException("data file missing", data_fname);
                }
                std::string index_fname = prefix + "index";
                std::string keeper_fname = std::move(prefix += "keeper");
                if (!env_io::fileExist(index_fname) || !env_io::fileExist(keeper_fname)) { // repair
                    if (env_io::fileExist(index_fname)) {
                        env_io::deleteFile(index_fname);
                    }
                    if (env_io::fileExist(keeper_fname)) {
                        env_io::deleteFile(keeper_fname);
                    }
                    _meta.build(std::move(keeper_fname), DBMeta{});
                    _af.build(data_fname);
                    _rf.build(std::move(data_fname));
                    _index.build(std::move(index_fname), _rf.get());
                    _writer.build(_af.get(), _af->immut_length());

                    auto it = TableIterator::open(_rf.get());
                    while (true) {
                        it->next();
                        if (!it->valid()) {
                            break;
                        }
                        auto item = it->item();
                        if (it->info().del) {
                            _index->remove(item.first, OffsetToData{item.second});
                        } else {
                            _index->insert(item.first, OffsetToData{item.second});
                        }
                    }
                } else {
                    _meta.build(std::move(keeper_fname));
                    _af.build(data_fname);
                    _rf.build(std::move(data_fname));
                    _index.build(std::move(index_fname), _meta->immut_value().offset, _rf.get());
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
                _meta.build(std::move(prefix += "keeper"), DBMeta{});
            }
        }

        ~DBImpl() noexcept override {
            _meta->mut_value().offset = _index->immut_empty();
        }

    public:
        bool put(const Slice & key,
                 const Slice & value,
                 const PutOptions & options) override {
            try {
                auto bkv = LogWriter::makeRecord(key, value);
                uint32_t pos = _writer->addRecord(bkv);
                _index->insert(key, OffsetToData{pos});

                if (options.sync) {
                    sync();
                }
                return true;
            } catch (const IndexFullControlledException &) {
            } catch (const LogFullControlledException &) {
            }
            return false;
        }

        bool remove(const Slice & key,
                    const RemoveOptions & options) override {
            try {
                auto bkv = LogWriter::makeRecord(key, {});
                uint32_t pos = _writer->addRecordForDel(bkv);
                _index->remove(key, OffsetToData{pos});

                if (options.sync) {
                    sync();
                }
                return true;
            } catch (const IndexFullControlledException &) {
            } catch (const LogFullControlledException &) {
            }
            return false;
        }

        bool write(const std::pair<Slice, Slice> * kvs, size_t n,
                   const WriteOptions & options) override {
            assert(n != 0);
            try {
                bool compress = options.try_compress;
                size_t uncompress_size = 0;
                for (size_t i = 0; i < n; ++i) {
                    const auto & kv = kvs[i];
                    if (kv.second.data() == nullptr) {
                        compress = false;
                        break;
                    }
                    uncompress_size += kv.first.size() + kv.second.size();
                }

                if (compress) {
                    auto bkvs = LogWriter::makeCompressedRecords(kvs, n);
                    if (bkvs.size() <= uncompress_size - uncompress_size / kWorthCompressRatio) {
                        uint32_t pos = _writer->addCompressedRecords(bkvs);
                        for (size_t i = 0; i < n; ++i) {
                            _index->insert(kvs[i].first, OffsetToData{pos});
                        }
                        return true;
                    }
                }

                std::vector<std::vector<uint8_t>> bkvs;
                std::vector<Slice> slices;
                std::vector<uint32_t> addrs;
                bkvs.reserve(n);
                slices.reserve(n);
                addrs.reserve(n);

                for (size_t i = 0; i < n; ++i) {
                    const auto & kv = kvs[i];
                    bkvs.emplace_back(LogWriter::makeRecord(kv.first, kv.second));
                    slices.emplace_back(bkvs.back());
                    addrs.emplace_back(kv.second.data() == nullptr); // 1 = del
                }

                addrs = _writer->addRecordsMayDel(slices.data(), n, std::move(addrs));
                assert(addrs.size() == n);
                for (size_t i = 0; i < n; ++i) {
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
            } catch (const LogFullControlledException &) {
            }
            return false;
        }

        std::pair<std::string, bool>
        get(const Slice & key) const override {
            std::pair<std::string, bool> res;
            res.second = _index->find(key, &res.first);
            return res;
        };

        std::unique_ptr<Iterator<Slice, Slice, bool>>
        scan() const override {
            return _index->scan();
        };

        bool exist(const Slice & key) const override {
            return _index->find(key, nullptr);
        }

        void sync() override {
            _af->sync();
            _index->sync();
        }
    };

    std::unique_ptr<DB>
    DB::open(const std::string & name, const OpenOptions & options) {
        return std::make_unique<DBImpl>(name, options);
    }

    bool repairDB(const std::string & name, std::function<void(const Exception &, uint32_t)> reporter) noexcept {
        try {
            {
                RandomAccessFile rf(name + "/data");
                auto it = RecoveryIterator::open(&rf, reporter);
                auto tmp_db = DB::open(name + "_tmp", {true, true}/* create, error if exist */);
                std::map<std::string, std::string, SliceComparator> q;
                std::vector<std::pair<Slice, Slice>> slices;
                size_t uncompress_size = 0;

                while (true) {
                    it->next();
                    if (!it->valid()) {
                        break;
                    }

                    auto item = it->item();
                    if (it->info().del) {
                        tmp_db->remove(item.first, {});
                        auto find_res = q.find(item.first);
                        if (find_res != q.end()) {
                            uncompress_size -= (find_res->first.size() + find_res->second.size());
                            q.erase(find_res);
                        }
                    } else {
                        q[item.first.toString()] = item.second.toString();
                        uncompress_size += (item.first.size() + item.second.size());
                    }

                    if (uncompress_size >= kPageSize) {
                        slices.reserve(q.size());
                        for (const auto & kv:q) {
                            slices.emplace_back(kv.first, kv.second);
                        }
                        tmp_db->write(slices.data(), slices.size(), {});

                        q.clear();
                        slices.clear();
                        uncompress_size = 0;
                    }
                }

                if (uncompress_size != 0) {
                    slices.reserve(q.size());
                    for (const auto & kv:q) {
                        slices.emplace_back(kv.first, kv.second);
                    }
                    tmp_db->write(slices.data(), slices.size(), {});

                    q.clear();
                    slices.clear();
                }
            }

            destroyDB(name);
            env_io::renameFile(name + "_tmp", name);
            return true;
        } catch (const Exception & e) {
            reporter(e, kDiskNull);
        } catch (const std::exception &) {
        }
        return false;
    }

    void destroyDB(const std::string & name) {
        for (const std::string & child:env_io::getChildren(name)) {
            env_io::deleteFile((name + '/') += child);
        }
        env_io::deleteDir(name);
    }
}