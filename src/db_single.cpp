#include "db_single.h"
#include "log_reader.h"

namespace LeviDB {
    DBSingle::DBSingle(std::string name, Options options, SeqGenerator * seq_gen)
            : DB(std::move(name), options), _seq_gen(seq_gen) {
        std::string prefix = _name + '/';

        if (IOEnv::fileExists(_name)) {
            if (_options.error_if_exists) {
                throw Exception::invalidArgumentException("DB already exists");
            }
            // 打开现有数据库
            _file_lock.build(prefix + "lock");
            std::string data_fname = prefix + "data";
            if (!IOEnv::fileExists(data_fname)) {
                throw Exception::notFoundException("data file missing", data_fname);
            }
            std::string index_fname = prefix + "index";
            std::string keeper_fname = std::move(prefix) + "keeper";
            if (!IOEnv::fileExists(index_fname) || !IOEnv::fileExists(keeper_fname)) { // repair
                if (IOEnv::fileExists(index_fname)) {
                    IOEnv::deleteFile(index_fname);
                } else {
                    IOEnv::deleteFile(keeper_fname);
                }

                _meta.build(std::move(keeper_fname), DBSingleWeakMeta{}, "");
                _af.build(data_fname);
                _rf.build(std::move(data_fname));
                _index.build(std::move(index_fname), _seq_gen, _rf.get());
                auto it = LogReader::makeTableIteratorOffset(_rf.get());
                while (it->valid()) {
                    auto item = it->item();
                    updateKeyRange(item.first);
                    _index->insert(item.first, OffsetToData{item.second});
                    it->next();
                }
                _writer.build(_af.get());
                return;
            }
            _meta.build(std::move(keeper_fname));
            _af.build(data_fname);
            _rf.build(std::move(data_fname));
            _index.build(std::move(index_fname), _meta->immut_value()._offset, _seq_gen, _rf.get());
            _writer.build(_af.get());
        } else {
            if (!_options.create_if_missing) {
                throw Exception::notFoundException("DB not found");
            }
            // 新建数据库
            IOEnv::createDir(_name);
            _file_lock.build(prefix + "lock");
            _af.build(prefix + "data");
            _rf.build(prefix + "data");
            _index.build(prefix + "index", _seq_gen, _rf.get());
            _writer.build(_af.get());
            _meta.build(std::move(prefix) + "keeper", DBSingleWeakMeta{}, "");
        }
    }

    bool DBSingle::put(const WriteOptions & options,
                       const Slice & key,
                       const Slice & value) {
        assert(!options.compress);
        RWLockWriteGuard write_guard(_rwlock);

        if (key.size() == 0) {
            _af->sync();
            _index->sync();
            return true;
        }

        uint32_t pos = _writer->calcWritePos();
        std::vector<uint8_t> bin = LogWriter::makeRecord(key, value);
        if (_index->immut_dst().immut_length() > UINT32_MAX - 4096/* BDT page size */
            || pos + (bin.size() + LogWriterConst::header_size_) * 2 > UINT32_MAX) {
            return false;
        }
        _writer->addRecord({bin.data(), bin.size()});
        updateKeyRange(key);
        _index->insert(key, OffsetToData{pos});

        _index->tryApplyPending();
        if (options.sync) {
            _af->sync();
            _index->sync();
        };
        return true;
    };

    bool DBSingle::remove(const WriteOptions & options,
                          const Slice & key) {
        RWLockWriteGuard write_guard(_rwlock);

        if (_writer->calcWritePos() + (key.size() + LogWriterConst::header_size_) * 2 > UINT32_MAX) {
            return false;
        }
        std::vector<uint8_t> bin = LogWriter::makeRecord(key, {});
        _writer->addDelRecord({bin.data(), bin.size()});
        _index->remove(key);

        _index->tryApplyPending();
        if (options.sync) {
            _af->sync();
            _index->sync();
        };
        return true;
    };

    bool DBSingle::write(const WriteOptions & options,
                         const std::vector<std::pair<Slice, Slice>> & kvs) {
        RWLockWriteGuard write_guard(_rwlock);

        if (options.compress) {
            assert(options.uncompress_size != 0);
            uint32_t pos = _writer->calcWritePos();
            std::vector<uint8_t> bin = LogWriter::makeCompressRecord(kvs);
            if (bin.size() <= options.uncompress_size - options.uncompress_size / 8) { // worth
                if (_index->immut_dst().immut_length() +
                    (kvs.size() + IndexConst::rank_ - 1) / IndexConst::rank_ * 4096/* BDT page size */ * 3 > UINT32_MAX
                    || pos + (bin.size() + LogWriterConst::header_size_) * 2 > UINT32_MAX) {
                    return false;
                }
                _writer->addCompressRecord({bin.data(), bin.size()});
                for (const auto & kv:kvs) {
                    updateKeyRange(kv.first);
                    _index->insert(kv.first, OffsetToData{pos});
                }

                _index->tryApplyPending();
                if (options.sync) {
                    _af->sync();
                    _index->sync();
                };
                return true;
            }
        }

        uint32_t bin_size = 0;
        std::vector<std::vector<uint8_t>> group;
        group.reserve(kvs.size());
        for (const auto & kv:kvs) {
            group.emplace_back(LogWriter::makeRecord(kv.first, kv.second));
            bin_size += group.back().size();
        }

        if (_index->immut_dst().immut_length() +
            (kvs.size() + IndexConst::rank_ - 1) / IndexConst::rank_ * 4096/* BDT page size */ * 3 > UINT32_MAX
            || _writer->calcWritePos() + (bin_size + LogWriterConst::header_size_) * 2 > UINT32_MAX) {
            return false;
        }

        std::vector<Slice> bkvs;
        bkvs.reserve(group.size());
        for (const auto & bkv:group) {
            bkvs.emplace_back(bkv.data(), bkv.size());
        }

        std::vector<uint32_t> addrs = _writer->addRecords(bkvs);
        assert(kvs.size() == addrs.size());
        for (int i = 0; i < kvs.size(); ++i) {
            updateKeyRange(kvs[i].first);
            _index->insert(kvs[i].first, OffsetToData{addrs[i]});
        }

        _index->tryApplyPending();
        if (options.sync) {
            _af->sync();
            _index->sync();
        };
        return true;
    };

    std::pair<std::string, bool>
    DBSingle::get(const ReadOptions & options, const Slice & key) const {
        RWLockReadGuard read_guard(_rwlock);
        return _index->find(key, options.sequence_number);
    };

    std::unique_ptr<Snapshot>
    DBSingle::makeSnapshot() {
        RWLockWriteGuard write_guard(_rwlock);
        _index->tryApplyPending();
        return _seq_gen->makeSnapshot();
    };

    std::unique_ptr<Iterator<Slice, std::string>>
    DBSingle::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        RWLockReadGuard read_guard(_rwlock);
        return _index->makeIterator(std::move(snapshot));
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    DBSingle::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                std::unique_ptr<Snapshot> && snapshot) const {
        RWLockReadGuard read_guard(_rwlock);
        return _index->makeRegexIterator(std::move(regex), std::move(snapshot));
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    DBSingle::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {
        RWLockReadGuard read_guard(_rwlock);
        return _index->makeRegexReversedIterator(std::move(regex), std::move(snapshot));
    };

    void DBSingle::tryApplyPending() {
        RWLockWriteGuard write_guard(_rwlock);
        _index->tryApplyPending();
    }

    Slice DBSingle::largestKey() const {
        RWLockReadGuard read_guard(_rwlock);
        return largestKeyUnlocked();
    };

    Slice DBSingle::smallestKey() const {
        RWLockReadGuard read_guard(_rwlock);
        return smallestKeyUnlocked();
    };

    void DBSingle::explicitRemove(const WriteOptions & options, const Slice & key) {
        RWLockWriteGuard write_guard(_rwlock);

        uint32_t pos = _writer->calcWritePos();
        std::vector<uint8_t> bin = LogWriter::makeRecord(key, {});
        _writer->addDelRecord({bin.data(), bin.size()});
        _index->insert(key, OffsetToData{pos});

        _index->tryApplyPending();
        if (options.sync) {
            _af->sync();
            _index->sync();
        };
    }

// methods below don't need lock
    bool DBSingle::canRelease() const {
        return _index->immut_operating_iters() == 0 && _index->immut_pending().empty();
    }

    Slice DBSingle::largestKeyUnlocked() const noexcept {
        const std::string & trailing = _meta->immut_trailing();
        uint32_t from_k_len = _meta->immut_value().from_k_len;
        uint32_t to_k_len = _meta->immut_value().to_k_len;
        return {trailing.data() + from_k_len, to_k_len};
    };

    Slice DBSingle::smallestKeyUnlocked() const noexcept {
        const std::string & trailing = _meta->immut_trailing();
        uint32_t from_k_len = _meta->immut_value().from_k_len;
        return {trailing.data(), from_k_len};
    };

    void DBSingle::updateKeyRange(const Slice & key) noexcept {
        if (SliceComparator{}(key, smallestKeyUnlocked()) || smallestKeyUnlocked().size() == 0) { // find smaller
            std::string & trailing = _meta->mut_trailing();
            uint32_t from_k_len = _meta->immut_value().from_k_len;
            trailing.replace(trailing.begin(), trailing.begin() + from_k_len, key.data(), key.data() + key.size());
            _meta->mut_value().from_k_len = static_cast<uint32_t>(key.size());
        } else if (SliceComparator{}(largestKeyUnlocked(), key)) { // find larger
            std::string & trailing = _meta->mut_trailing();
            uint32_t from_k_len = _meta->immut_value().from_k_len;
            uint32_t to_k_len = _meta->immut_value().to_k_len;
            trailing.replace(trailing.begin() + from_k_len, trailing.begin() + from_k_len + to_k_len,
                             key.data(), key.data() + key.size());
            _meta->mut_value().to_k_len = static_cast<uint32_t>(key.size());
        }
    };

    bool repairDBSingle(const std::string & db_single_name, reporter_t reporter) noexcept {
        try {
            {
                RandomAccessFile rf(db_single_name + "/data");
                auto it = LogReader::makeTableRecoveryIteratorKV(&rf, reporter);

                SeqGenerator seq_gen;
                Options options{};
                options.create_if_missing = true;
                options.error_if_exists = true;
                DBSingle db(db_single_name + "_temp", options, &seq_gen);

                while (it->valid()) {
                    auto item = it->item();
                    if (item.second.back() == 1) { // del
                        db.explicitRemove(WriteOptions{}, item.first);
                    } else {
                        item.second.pop_back();
                        db.put(WriteOptions{}, item.first, item.second);
                    }
                    it->next();
                }
            }
            if (IOEnv::fileExists(db_single_name)) {
                for (const std::string & child:IOEnv::getChildren(db_single_name)) {
                    IOEnv::deleteFile((db_single_name + '/') += child);
                }
                IOEnv::deleteDir(db_single_name);
            }
            IOEnv::renameFile(db_single_name + "_temp", db_single_name);

        } catch (const Exception & e) {
            reporter(e);
            return false;
        }
        return true;
    };
}
