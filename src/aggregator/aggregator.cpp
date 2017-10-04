#include <algorithm>

#include "../db_single.h"
#include "aggregator.h"
#include "compact_1_2.h"

namespace LeviDB {
    Aggregator::Aggregator(std::string name, Options options)
            : DB(std::move(name), options) {
        std::string prefix = _name + '/';

        if (IOEnv::fileExists(_name)) {
            if (_options.error_if_exists) {
                throw Exception::invalidArgumentException("DB already exists");
            }
            // 打开现有数据库
            _file_lock.build(prefix + "lock");
            _meta.build(prefix + "keeper");

            // 确认兼容
            if (_meta->immut_value().format_version > AggregatorStrongMeta{}.format_version ||
                _meta->immut_value().db_version > AggregatorStrongMeta{}.db_version) {
                throw Exception::invalidArgumentException("target's format is not supported");
            }

            // 获取分片信息并简单修复
            std::vector<std::string> children = IOEnv::getChildren(prefix);
            for (const std::string & child:children) {
                std::string prefixed_child;
                if (not(child[0] >= '0' && child[0] <= '9')
                    || (prefixed_child = prefix + child, !IOEnv::fileExists(prefixed_child))) {
                    continue;
                }

                decltype(child.find(char{})) pos{};
                if ((pos = child.find('+')) != std::string::npos) { // compact 2 to 1
                    if (child.back() != '-') { // fail, remove product
                        for (const std::string & c:LeviDB::IOEnv::getChildren(prefixed_child)) {
                            LeviDB::IOEnv::deleteFile((prefixed_child + '/') += c);
                        }
                        LeviDB::IOEnv::deleteDir(prefixed_child);
                    } else { // success, remove resources
                        for (const std::string & n:{std::string(prefix).append(child.cbegin(), child.cbegin() + pos),
                                                    std::string(prefix).append(
                                                            child.cbegin() + (pos + 1), --child.cend())}) {
                            for (const std::string & c:LeviDB::IOEnv::getChildren(n)) {
                                LeviDB::IOEnv::deleteFile((n + '/') += c);
                            }
                            LeviDB::IOEnv::deleteDir(n);
                        }
                    }
                } else if ((pos = child.find('_')) != std::string::npos) { // compact 1 to 2
                    prefixed_child.resize(prefixed_child.size() - (child.size() - pos));
                    repairCompacting1To2DB(prefixed_child, [](const Exception & e) { throw e; });
                }
            }

            children = IOEnv::getChildren(prefix);
            children.erase(std::remove_if(children.begin(), children.end(), [&prefix](std::string & child) noexcept {
                child = prefix + child;
                return not(child.back() >= '0' && child.back() <= '9');
            }), children.end());
            // 写入 search_map
            for (std::string & child:children) {
                WeakKeeper<DBSingleWeakMeta> m(child + "/keeper");
                const std::string & trailing = m.immut_trailing();
                uint32_t from_k_len = m.immut_value().from_k_len;

                auto node = std::make_unique<AggregatorNode>();
                node->lower_bound = std::string(m.immut_trailing().data(),
                                                m.immut_trailing().data() + from_k_len);
                node->db_name = std::move(child);
                insertNodeUnlocked(std::move(node));
            }
        } else {
            if (!_options.create_if_missing) {
                throw Exception::notFoundException("DB not found");
            }
            // 新建数据库
            IOEnv::createDir(_name);
            _file_lock.build(prefix + "lock");
            _meta.build(prefix + "keeper", AggregatorStrongMeta{}, std::string{});

            // 至少存在一个数据库分片
            auto node = std::make_unique<AggregatorNode>();
            Options opt;
            opt.create_if_missing = true;
            opt.error_if_exists = true;
            node->db = std::make_unique<DBSingle>(std::to_string(_meta->immut_value().counter), opt, &_seq_gen);
            _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
            node->db_name = node->db->immut_name();
        }

        // 日志
        std::string logger_prev_fname = prefix + "log_prev.txt";
        if (IOEnv::fileExists(logger_prev_fname)) {
            IOEnv::deleteFile(logger_prev_fname);
        }
        std::string logger_fname = std::move(prefix) + "log.txt";
        if (IOEnv::fileExists(logger_fname)) {
            IOEnv::renameFile(logger_fname, logger_prev_fname);
        }
        _logger.build(logger_fname);
        Logger::logForMan(_logger.get(), "start OK");
    };

    Aggregator::~Aggregator() noexcept {
        Logger::logForMan(_logger.get(), "rename dbs");
        AggregatorNode * cursor = &_head;
        while (true) {
            cursor = cursor->next.get();
            if (cursor == nullptr) {
                break;
            }
            if (cursor->db) {
                Logger::logForMan(_logger.get(), "rename %s to %llu",
                                  cursor->db_name.c_str(),
                                  _meta->immut_value().counter);
                std::string name = std::move(cursor->db->mut_name());
                cursor->db = nullptr;
                IOEnv::renameFile(name, std::to_string(_meta->immut_value().counter));
                _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
            }
        }
        Logger::logForMan(_logger.get(), "end OK");
    }

    static void mayOpenDB(AggregatorNode * match, SeqGenerator * seq_gen) {
        if (match->db == nullptr) { // open DB
            match->db = std::make_unique<DBSingle>(match->db_name, Options{}, seq_gen);
        }
    }

    static void ifCompact1To2Done(AggregatorNode * match) {
        if (match->db->immut_name().empty() && match->db->canRelease()) { // compact 1 to 2 done
            auto * compact_db = static_cast<Compacting1To2DB *>(match->db.get());
            compact_db->syncFiles();
            std::unique_ptr<DB> product_a = std::move(compact_db->mut_product_a());
            std::unique_ptr<DB> product_b = std::move(compact_db->mut_product_b());
            match->db = std::move(product_a);

            auto next_node = std::make_unique<AggregatorNode>();
            next_node->db = std::move(product_b);
            next_node->db_name = next_node->db->immut_name();
            next_node->lower_bound = next_node->db->smallestKey().toString();
            next_node->next = std::move(match->next);
            match->next = std::move(next_node);
        }
    }

    bool Aggregator::put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) {
        auto find_res = findBestMatchForWrite(key);
        AggregatorNode * match = find_res.first;

        mayOpenDB(match, &_seq_gen);
        if (!match->db->put(options, key, value)) {
            Logger::logForMan(_logger.get(), "split %s when put", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db->put(options, key, value);
        }
        ifCompact1To2Done(match);
        return true;
    };

    bool Aggregator::remove(const WriteOptions & options,
                            const Slice & key) {
        auto find_res = findBestMatchForWrite(key);
        AggregatorNode * match = find_res.first;

        mayOpenDB(match, &_seq_gen);
        if (!match->db->remove(options, key)) {
            Logger::logForMan(_logger.get(), "split %s when remove", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db->remove(options, key);
        }
        ifCompact1To2Done(match);
        return true;
    };

    bool Aggregator::write(const WriteOptions & options,
                           const std::vector<std::pair<Slice, Slice>> & kvs) {
        auto find_res = findBestMatchForWrite(kvs.front().first);
        AggregatorNode * match = find_res.first;

        mayOpenDB(match, &_seq_gen);
        if (!match->db->write(options, kvs)) {
            Logger::logForMan(_logger.get(), "split %s when write()", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db->write(options, kvs);
        }

        WriteOptions opt{};
        opt.sync = options.sync;
        AggregatorNode * cursor = match;
        while (true) {
            AggregatorNode * next_cursor = cursor->next.get();
            if (next_cursor != nullptr) {
                auto it = std::lower_bound(kvs.cbegin(), kvs.cend(), next_cursor->lower_bound, SliceComparator{});
                if (it == kvs.cend()) {
                    break;
                }
                RWLockWriteGuard next_write_guard(next_cursor->lock);
                mayOpenDB(next_cursor, &_seq_gen);

                AggregatorNode * next_next_cursor = next_cursor->next.get();
                if (next_next_cursor != nullptr) {
                    RWLockReadGuard next_next_read_guard(next_next_cursor->lock);

                    while (it != kvs.cend() && SliceComparator{}(it->first, next_next_cursor->lower_bound)) {
                        next_cursor->db->put(opt, it->first, it->second);
                        ++it;
                    }
                } else {
                    while (it != kvs.cend()) {
                        next_cursor->db->put(opt, it->first, it->second);
                        ++it;
                    }
                }

                cursor = next_cursor;
            } else {
                break;
            }
        }

        // don't need any lock here, because we have locked next_cursor once
        // no other threads can reach here
        AggregatorNode * next_cursor = cursor->next.get();
        if (next_cursor != nullptr) {
            auto it = std::lower_bound(kvs.cbegin(), kvs.cend(), next_cursor->lower_bound, SliceComparator{});
            while (it != kvs.cend()) {
                match->db->remove(opt, it->first);
            }
        }
        match->db->updateKeyRange();
        return true;
    };

    std::pair<std::string, bool>
    Aggregator::get(const ReadOptions & options, const Slice & key) const {
        auto find_res = findBestMatchForRead(key);
        return find_res.first->db->get(options, key);
    };

    std::unique_ptr<Snapshot>
    Aggregator::makeSnapshot() {
        return _seq_gen.makeSnapshot();
    };

    bool Aggregator::explicitRemove(const WriteOptions & options,
                                    const Slice & key) {
        auto find_res = findBestMatchForWrite(key);
        AggregatorNode * match = find_res.first;

        mayOpenDB(match, &_seq_gen);
        if (!match->db->explicitRemove(options, key)) {
            Logger::logForMan(_logger.get(), "split %s when e-remove", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db->explicitRemove(options, key);
        }
        ifCompact1To2Done(match);
        return true;
    };

    void Aggregator::insertNodeUnlocked(std::unique_ptr<AggregatorNode> && node) noexcept {

    };

    std::pair<AggregatorNode *, RWLockWriteGuard>
    Aggregator::findBestMatchForWrite(const Slice & target) {
        bool expected = true;
        if (_ready_gc.compare_exchange_strong(expected, false)) {
            gc();
        }
    };

    std::pair<AggregatorNode *, RWLockReadGuard>
    Aggregator::findBestMatchForRead(const Slice & target) const {

    };

    void Aggregator::gc() {

    }

    bool repairDB(const std::string & db_name, reporter_t reporter) noexcept {

    };
}