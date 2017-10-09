#include <algorithm>

#include "../db_single.h"
#include "aggregator.h"
#include "compact_1_2.h"
#include "compact_2_1.h"

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
            std::vector<std::string> children = IOEnv::getChildren(_name);
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

            children = IOEnv::getChildren(_name);
            children.erase(std::remove_if(children.begin(), children.end(), [&prefix](std::string & child) noexcept {
                return not(child[0] >= '0' && child[0] <= '9') || (child = prefix + child, false);
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
            insertNodeUnlocked(std::move(node));
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
        AggregatorNode * cursor = &_head;
        while (true) {
            cursor = cursor->next.get();
            if (cursor == nullptr) {
                break;
            }
            if (cursor->db != nullptr) {
                cursor->db->tryApplyPending();
                assert(cursor->db->canRelease());
                cursor->db = nullptr;
            }

            if (!cursor->db_name.empty() &&
                not(cursor->db_name.back() >= '0' && cursor->db_name.back() <= '9')) {
                Logger::logForMan(_logger.get(), "rename %s to %llu",
                                  cursor->db_name.c_str(),
                                  static_cast<unsigned long long>(_meta->immut_value().counter));
                try {
                    IOEnv::renameFile(cursor->db_name, std::to_string(_meta->immut_value().counter));
                } catch (const Exception & e) {
                    Logger::logForMan(_logger.get(), "rename %s to %llu failed, because %s",
                                      cursor->db_name.c_str(),
                                      static_cast<unsigned long long>(_meta->immut_value().counter),
                                      e.toString().c_str());
                }
                _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
            }
        }
        Logger::logForMan(_logger.get(), "end OK");
    }

    static void mayOpenDB(AggregatorNode * match, SeqGenerator * seq_gen) {
        if (match->db == nullptr) {
            match->db = std::make_unique<DBSingle>(match->db_name, Options{}, seq_gen);
        }
    }

    static void ifCompact1To2Done(AggregatorNode * match) {
        if (match->db->immut_name().empty() && match->db->canRelease()) {
            assert(match->db_name.empty());
            auto * compact_db = static_cast<Compacting1To2DB *>(match->db.get());
            compact_db->syncFiles();
            std::unique_ptr<DB> product_a = std::move(compact_db->mut_product_a());
            std::unique_ptr<DB> product_b = std::move(compact_db->mut_product_b());
            match->db = std::move(product_a);
            match->db_name = match->db->immut_name();

            auto next_node = std::make_unique<AggregatorNode>();
            next_node->db = std::move(product_b);
            next_node->db_name = next_node->db->immut_name();
            next_node->lower_bound = next_node->db->smallestKey().toString();
            next_node->next = std::move(match->next);
            next_node->hit = (match->hit = match->hit / 2);
            match->next = std::move(next_node);
        }
    }

    bool Aggregator::put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) {
        RWLockWriteGuard guard;
        AggregatorNode * match = findBestMatchForWrite(key, &guard);

        mayOpenDB(match, &_seq_gen);
        if (!match->db->put(options, key, value)) {
            Logger::logForMan(_logger.get(), "split %s when put", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db_name.clear();
            match->db->put(options, key, value);
        }
        ifCompact1To2Done(match);
        return true;
    };

    bool Aggregator::remove(const WriteOptions & options,
                            const Slice & key) {
        RWLockWriteGuard guard;
        AggregatorNode * match = findBestMatchForWrite(key, &guard);

        mayOpenDB(match, &_seq_gen);
        if (!match->db->remove(options, key)) {
            Logger::logForMan(_logger.get(), "split %s when remove", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db_name.clear();
            match->db->remove(options, key);
        }
        ifCompact1To2Done(match);
        return true;
    };

    bool Aggregator::write(const WriteOptions & options,
                           const std::vector<std::pair<Slice, Slice>> & kvs) {
        RWLockWriteGuard guard;
        AggregatorNode * match = findBestMatchForWrite(kvs.front().first, &guard);

        mayOpenDB(match, &_seq_gen);
        if (!match->db->write(options, kvs)) {
            Logger::logForMan(_logger.get(), "split %s when write", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db_name.clear();
            match->db->write(options, kvs);
        }

        WriteOptions opt{};
        opt.sync = options.sync;
        AggregatorNode * cursor = match;
        while (true) {
            AggregatorNode * next_cursor = cursor->next.get();
            if (next_cursor != nullptr) {
                auto it = std::lower_bound(kvs.begin(), kvs.end(),
                                           std::make_pair(Slice(next_cursor->lower_bound), Slice()),
                                           [](const std::pair<Slice, Slice> & a,
                                              const std::pair<Slice, Slice> & b) noexcept {
                                               return SliceComparator{}(a.first, b.first);
                                           });
                if (it == kvs.cend()) {
                    break;
                }
                RWLockWriteGuard next_guard(next_cursor->lock);
                mayOpenDB(next_cursor, &_seq_gen);

                AggregatorNode * next_next_cursor = next_cursor->next.get();
                if (next_next_cursor != nullptr) {
                    while (it != kvs.cend() && SliceComparator{}(it->first, next_next_cursor->lower_bound)) {
                        if (!next_cursor->db->put(opt, it->first, it->second)) {
                            Logger::logForMan(_logger.get(), "split %s when put inside write",
                                              next_cursor->db_name.c_str());
                            next_cursor->db = std::make_unique<Compacting1To2DB>(std::move(next_cursor->db), &_seq_gen);
                            next_cursor->db_name.clear();
                            next_cursor->db->put(opt, it->first, it->second);
                        }
                        ++it;
                    }
                } else {
                    while (it != kvs.cend()) {
                        if (!next_cursor->db->put(opt, it->first, it->second)) {
                            Logger::logForMan(_logger.get(), "split %s when put inside write",
                                              next_cursor->db_name.c_str());
                            next_cursor->db = std::make_unique<Compacting1To2DB>(std::move(next_cursor->db), &_seq_gen);
                            next_cursor->db_name.clear();
                            next_cursor->db->put(opt, it->first, it->second);
                        }
                        ++it;
                    }
                }

                cursor = next_cursor;
            } else {
                break;
            }
        }

        AggregatorNode * next_cursor = match->next.get();
        if (next_cursor != nullptr) {
            auto it = std::lower_bound(kvs.begin(), kvs.end(),
                                       std::make_pair(Slice(next_cursor->lower_bound), Slice()),
                                       [](const std::pair<Slice, Slice> & a,
                                          const std::pair<Slice, Slice> & b) noexcept {
                                           return SliceComparator{}(a.first, b.first);
                                       });
            while (it != kvs.cend()) {
                if (!match->db->remove(opt, it->first)) {
                    Logger::logForMan(_logger.get(), "split %s when remove inside write", match->db_name.c_str());
                    match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
                    match->db_name.clear();
                    match->db->remove(opt, it->first);
                }
                ++it;
            }
        }
        match->db->updateKeyRange();
        return true;
    };

    std::pair<std::string, bool>
    Aggregator::get(const ReadOptions & options, const Slice & key) const {
        RWLockReadGuard read_guard;
        const AggregatorNode * match = findBestMatchForRead(key, &read_guard);
        if (match->db == nullptr) {
            {
                RWLockReadGuard _(std::move(read_guard));
            }
            RWLockWriteGuard write_guard;
            AggregatorNode * m = const_cast<Aggregator *>(this)->findBestMatchForWrite(key, &write_guard);
            mayOpenDB(m, &const_cast<Aggregator *>(this)->_seq_gen);
            return m->db->get(options, key);
        }
        return match->db->get(options, key);
    };

    std::unique_ptr<Snapshot>
    Aggregator::makeSnapshot() {
        return _seq_gen.makeSnapshot();
    };

    bool Aggregator::explicitRemove(const WriteOptions & options,
                                    const Slice & key) {
        RWLockWriteGuard guard;
        AggregatorNode * match = findBestMatchForWrite(key, &guard);

        mayOpenDB(match, &_seq_gen);
        if (!match->db->explicitRemove(options, key)) {
            Logger::logForMan(_logger.get(), "split %s when explicit remove", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db->explicitRemove(options, key);
        }
        ifCompact1To2Done(match);
        return true;
    };

    void Aggregator::insertNodeUnlocked(std::unique_ptr<AggregatorNode> && node) noexcept {
        AggregatorNode * prev = &_head;
        AggregatorNode * cursor{};
        while (true) {
            cursor = prev->next.get();
            if (cursor != nullptr && SliceComparator{}(cursor->lower_bound, node->lower_bound)) {
                prev = cursor;
            } else {
                node->next = std::move(prev->next);
                prev->next = std::move(node);
                break;
            }
        }
    };

    AggregatorNode * Aggregator::findBestMatchForWrite(const Slice & target, RWLockWriteGuard * lock) {
        bool expected = true;
        if (_ready_gc.compare_exchange_strong(expected, false)) {
            gc();
        }

        int open_cnt = 0;
        AggregatorNode * cursor = &_head;
        AggregatorNode * next{};
        RWLockWriteGuard write_guard;
        while (true) {
            next = cursor->next.get();
            if (next == nullptr ||
                (open_cnt += static_cast<int>(next->db != nullptr), SliceComparator{}(target, next->lower_bound))) {
                *lock = std::move(write_guard);
                ++cursor->hit;
                break;
            }
            write_guard = RWLockWriteGuard(next->lock);
            cursor = next;
        }

        if (open_cnt > AggregatorConst::max_dbs_) {
            bool expect = false;
            _ready_gc.compare_exchange_strong(expect, true);
        }
        return cursor;
    };

    const AggregatorNode * Aggregator::findBestMatchForRead(const Slice & target, RWLockReadGuard * lock) const {
        int open_cnt = 0;
        const AggregatorNode * cursor = &_head;
        const AggregatorNode * next{};
        RWLockReadGuard read_guard;
        while (true) {
            next = cursor->next.get();
            if (next == nullptr ||
                (open_cnt += static_cast<int>(next->db != nullptr), SliceComparator{}(target, next->lower_bound))) {
                *lock = std::move(read_guard);
                ++cursor->hit;
                break;
            }
            read_guard = RWLockReadGuard(next->lock);
            cursor = next;
        }

        if (open_cnt > AggregatorConst::max_dbs_) {
            bool expect = false;
            _ready_gc.compare_exchange_strong(expect, true);
        }
        return cursor;
    };

    void Aggregator::gc() {
        std::vector<AggregatorNode *> gc_q;
        RWLockWriteGuard fixed_guard(_head.next->lock);

        AggregatorNode * cursor = _head.next.get();
        AggregatorNode * next{};
        RWLockWriteGuard move_guard;
        while (true) {
            if (cursor->db != nullptr) {
                gc_q.emplace_back(cursor);
            }

            next = cursor->next.get();
            if (next == nullptr) {
                break;
            }
            move_guard = RWLockWriteGuard(next->lock);
            if (cursor->db != nullptr && next->db != nullptr
                && !cursor->db->immut_name().empty() && !next->db->immut_name().empty()
                && cursor->db->canRelease() && next->db->canRelease()
                && static_cast<DBSingle *>(cursor->db.get())->spaceUsage() +
                   static_cast<DBSingle *>(next->db.get())->spaceUsage() < AggregatorConst::merge_threshold_) {
                Compacting2To1Worker worker(std::move(cursor->db), std::move(next->db), &_seq_gen);
                cursor->db = std::move(worker.mut_product());
                cursor->db_name = cursor->db->immut_name();

                cursor->hit += next->hit;
                cursor->next = std::move(next->next);
            }
            cursor = next;
        }

        std::sort(gc_q.begin(), gc_q.end(), [](const AggregatorNode * a,
                                               const AggregatorNode * b) noexcept { return a->hit < b->hit; });
        size_t curr_dbs = gc_q.size();
        for (AggregatorNode * node:gc_q) {
            node->db->tryApplyPending();
            if (node->db->canRelease()) {
                node->db = nullptr;
                if (--curr_dbs <= AggregatorConst::max_dbs_ / 2) {
                    break;
                }
            }
        }
    }

    bool repairDB(const std::string & db_name, reporter_t reporter) noexcept {
        static constexpr char tmp_postfix[] = "_tmp";
        try {
            uint64_t max_num = 0;
            for (const std::string & child:IOEnv::getChildren(db_name)) {
                if (child[0] >= '0' && child[0] <= '9') { // find db
                    std::string prefixed_child = (db_name + '/') += child;
                    if (child.size() > sizeof(tmp_postfix) &&
                        std::equal(tmp_postfix, tmp_postfix + sizeof(tmp_postfix),
                                   child.cend() - sizeof(tmp_postfix), child.cend())) { // temp files
                        for (const std::string & c:LeviDB::IOEnv::getChildren(prefixed_child)) {
                            LeviDB::IOEnv::deleteFile((prefixed_child + '/') += c);
                        }
                        LeviDB::IOEnv::deleteDir(prefixed_child);
                    } else {
                        max_num = std::max<unsigned long long>(max_num, std::stoull(child));
                        if (!repairDBSingle(prefixed_child, reporter)) {
                            return false;
                        };
                    }
                }
            }

            for (const std::string & keeper_name:{db_name + "/keeper_a", db_name + "/keeper_b"}) {
                if (IOEnv::fileExists(keeper_name)) {
                    IOEnv::deleteFile(keeper_name);
                }
            }
            AggregatorStrongMeta meta{};
            meta.counter = max_num + 1;
            StrongKeeper<AggregatorStrongMeta>(db_name + "/keeper", meta, std::string{});
        } catch (const Exception & e) {
            reporter(e);
            return false;
        }
        return true;
    };
}