#include <algorithm>

#include "../db_single.h"
#include "aggregator.h"
#include "compact_1_2.h"
#include "compact_2_1.h"

namespace LeviDB {
    Aggregator::Aggregator(std::string name, Options options) : DB(std::move(name), options) {
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
            for (const std::string & child:IOEnv::getChildren(_name)) {
                std::string prefixed_child;
                if (not(child[0] >= '0' && child[0] <= '9')
                    || (prefixed_child = prefix + child, !IOEnv::fileExists(prefixed_child))) {
                    continue;
                }

                decltype(child.find(char{})) pos{};
                if ((pos = child.find('+')) != std::string::npos) { // compact 2 to 1
                    if (child.back() != '-') { // fail, remove product
                        for (const std::string & c:IOEnv::getChildren(prefixed_child)) {
                            IOEnv::deleteFile((prefixed_child + '/') += c);
                        }
                        IOEnv::deleteDir(prefixed_child);
                    } else { // success, remove resources
                        for (const std::string & n:{std::string(prefix).append(child.cbegin(), child.cbegin() + pos),
                                                    std::string(prefix).append(
                                                            child.cbegin() + (pos + 1), --child.cend())}) {
                            if (IOEnv::fileExists(n)) {
                                for (const std::string & c:IOEnv::getChildren(n)) {
                                    IOEnv::deleteFile((n + '/') += c);
                                }
                                IOEnv::deleteDir(n);
                            }
                        }
                    }
                } else if ((pos = child.find('_')) != std::string::npos) { // compact 1 to 2
                    prefixed_child.resize(prefixed_child.size() - (child.size() - pos));
                    if (IOEnv::fileExists(prefixed_child)) {
                        if (!repairCompacting1To2DB(prefixed_child, [](const Exception & e) noexcept {})) {
                            throw Exception::corruptionException("repairCompacting1To2DB failed", prefixed_child);
                        };
                    }
                }
            }

            std::vector<std::string> children = IOEnv::getChildren(_name);
            std::sort(children.begin(), children.end());
            children.erase(std::remove_if(children.begin(), children.end(), [&prefix, this]
                    (std::string & child) {
                if (child[0] >= '0' && child[0] <= '9') { // is db
                    std::string prefixed_child = prefix + child;
                    if (child.find('+') != std::string::npos || child.find('_') != std::string::npos) {
                        IOEnv::renameFile(prefixed_child,
                                          child = prefix + std::to_string(_meta->immut_value().counter));
                        _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
                    } else {
                        child = std::move(prefixed_child);
                    }
                    return false;
                }
                return true;
            }), children.end());

            // 写入 search_map
            for (std::string & child:children) {
                WeakKeeper<DBSingleWeakMeta> m(child + "/keeper");
                const std::string & trailing = m.immut_trailing();
                uint32_t from_k_len = m.immut_value().from_k_len;

                auto node = std::make_shared<AggregatorNode>();
                node->db_name = std::move(child);
                _dispatcher[std::string(trailing.data(), trailing.data() + from_k_len)] = node;
            }
        } else {
            if (!_options.create_if_missing) {
                throw Exception::notFoundException("DB not found");
            }
            // 新建数据库
            IOEnv::createDir(_name);
            _file_lock.build(prefix + "lock");
            _meta.build(prefix + "keeper", AggregatorStrongMeta{}/* counter starts from 1 */, std::string{});

            // 至少存在一个数据库分片
            auto node = std::make_shared<AggregatorNode>();
            Options opt;
            opt.create_if_missing = true;
            opt.error_if_exists = true;
            node->db = std::make_unique<DBSingle>(prefix + '0', opt, &_seq_gen);
            node->db_name = node->db->immut_name();
            _dispatcher[std::string{}] = node;
            ++_operating_dbs;
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
    }

    Aggregator::~Aggregator() noexcept {
        // 如果用户正确使用, 这里就不存在 race 的可能性
        // 因此不需要任何同步机制
        for (auto & it:_dispatcher) {
            auto node = it.second;
            if (node->db != nullptr) {
                node->db->tryApplyPending();
                assert(node->db->canRelease());
                node->db = nullptr;
#ifndef NDEBUG
                --_operating_dbs;
#endif
            }
        }
        assert(_operating_dbs == 0);
        Logger::logForMan(_logger.get(), "%llu dbs, end OK",
                          static_cast<unsigned long long>(_dispatcher.size()));
    }

    bool Aggregator::put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) {
        RWLockWriteGuard guard;
        auto match = findBestMatchForWrite(key, &guard);

        mayOpenDB(match);
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
        auto match = findBestMatchForWrite(key, &guard);

        mayOpenDB(match);
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
        std::string bound;
        auto match = findBestMatchForWrite(kvs.front().first, &guard, &bound);

        mayOpenDB(match);
        if (!match->db->write(options, kvs)) {
            Logger::logForMan(_logger.get(), "split %s when write", match->db_name.c_str());
            match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
            match->db_name.clear();
            match->db->write(options, kvs);
        }

        auto cmp = [](const std::pair<Slice, Slice> & a,
                      const std::pair<Slice, Slice> & b) noexcept {
            return SliceComparator{}(a.first, b.first);
        };

        WriteOptions opt;
        opt.sync = options.sync;
        RWLockWriteGuard cursor_guard;
        std::string cursor_bound = bound;
        while (true) {
            auto cursor = findNextOfBestMatchForWrite(cursor_bound, &cursor_guard, &cursor_bound);
            if (cursor != nullptr) {
                auto it = std::lower_bound(kvs.cbegin(), kvs.cend(), std::make_pair(Slice(cursor_bound), Slice()), cmp);
                if (it == kvs.cend()) {
                    break;
                }
                mayOpenDB(cursor);

                RWLockReadGuard next_guard;
                std::string next_bound;
                auto next = findNextOfBestMatchForRead(cursor_bound, &next_guard, &next_bound);
                if (next != nullptr) {
                    std::vector<std::pair<Slice, Slice>> part;
                    while (it != kvs.cend() && SliceComparator{}(it->first, next_bound)) {
                        part.emplace_back(it->first, it->second);
                        ++it;
                    }
                    if (!part.empty()) {
                        if (!cursor->db->write(opt, part)) {
                            Logger::logForMan(_logger.get(), "split %s when part-write inside write",
                                              cursor->db_name.c_str());
                            cursor->db = std::make_unique<Compacting1To2DB>(std::move(cursor->db), &_seq_gen);
                            cursor->db_name.clear();
                            cursor->db->write(opt, part);
                        }
                    }
                } else {
                    std::vector<std::pair<Slice, Slice>> part;
                    while (it != kvs.cend()) {
                        part.emplace_back(it->first, it->second);
                        ++it;
                    }
                    if (!part.empty()) {
                        if (!cursor->db->write(opt, part)) {
                            Logger::logForMan(_logger.get(), "split %s when part-write inside write",
                                              cursor->db_name.c_str());
                            cursor->db = std::make_unique<Compacting1To2DB>(std::move(cursor->db), &_seq_gen);
                            cursor->db_name.clear();
                            cursor->db->write(opt, part);
                        }
                    }
                }
            } else {
                break;
            }
        }
        {
            RWLockWriteGuard _(std::move(cursor_guard));
        }

        RWLockReadGuard next_guard;
        std::string next_bound;
        auto next = findNextOfBestMatchForRead(bound, &next_guard, &next_bound);
        if (next != nullptr) {
            auto it = std::lower_bound(kvs.cbegin(), kvs.cend(), std::make_pair(Slice(next_bound), Slice()), cmp);
            while (it != kvs.cend()) {
                if (it->second.data() == nullptr) {
                } else if (!match->db->remove(opt, it->first)) {
                    Logger::logForMan(_logger.get(), "split %s when remove inside write", match->db_name.c_str());
                    match->db = std::make_unique<Compacting1To2DB>(std::move(match->db), &_seq_gen);
                    match->db_name.clear();
                    match->db->remove(opt, it->first);
                }
                ++it;
            }
        }
        ifCompact1To2Done(match);
        return true;
    };

    std::pair<std::string, bool>
    Aggregator::get(const ReadOptions & options, const Slice & key) const {
        RWLockReadGuard read_guard;
        auto match = findBestMatchForRead(key, &read_guard);
        if (match->db == nullptr) {
            {
                RWLockReadGuard _(std::move(read_guard));
            }
            RWLockWriteGuard write_guard;
            auto m = const_cast<Aggregator *>(this)->findBestMatchForWrite(key, &write_guard);
            const_cast<Aggregator *>(this)->mayOpenDB(m);
            return m->db->get(options, key);
        }
        return match->db->get(options, key);
    };

    std::unique_ptr<Snapshot>
    Aggregator::makeSnapshot() {
        return _seq_gen.makeSnapshot();
    };

    std::string Aggregator::getProperty() const noexcept {
        std::string res;
        for (const auto & info:_dispatcher) {
            res += "lower_bound: ";
            res += info.first;
            if (info.second->db != nullptr) {
                res += " db_lower_bound: ";
                res.append(info.second->db->smallestKey().data(),
                           info.second->db->smallestKey().data() + info.second->db->smallestKey().size());
                res += " db_upper_bound: ";
                res.append(info.second->db->largestKey().data(),
                           info.second->db->largestKey().data() + info.second->db->largestKey().size());
            }
            res += '\n';
        }
        return res;
    }

    std::shared_ptr<AggregatorNode>
    Aggregator::findBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                                      std::string * lower_bound) {
        if (_operating_dbs > AggregatorConst::max_dbs_) {
            bool expected = false;
            if (_gc.compare_exchange_strong(expected, true)) {
                gc();
                assert(_gc);
                _gc = false;
            }
        }

        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }
            res = find_res->second;
            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockWriteGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        ++res->hit;
        return res;
    };

    std::shared_ptr<AggregatorNode>
    Aggregator::findPrevOfBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                                            std::string * lower_bound) {
        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }

            if (find_res == _dispatcher.begin()) { return nullptr; }
            --find_res;
            res = find_res->second;

            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockWriteGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        return res;
    };

    std::shared_ptr<AggregatorNode>
    Aggregator::findNextOfBestMatchForWrite(const Slice & target, RWLockWriteGuard * guard,
                                            std::string * lower_bound) {
        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }

            ++find_res;
            if (find_res == _dispatcher.end()) { return nullptr; }
            res = find_res->second;

            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockWriteGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        return res;
    };

    const std::shared_ptr<AggregatorNode>
    Aggregator::findBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                                     std::string * lower_bound) const {
        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }
            res = find_res->second;
            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockReadGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        ++res->hit;
        return res;
    };

    const std::shared_ptr<AggregatorNode>
    Aggregator::findPrevOfBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                                           std::string * lower_bound) const {
        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }

            if (find_res == _dispatcher.begin()) { return nullptr; }
            --find_res;
            res = find_res->second;

            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockReadGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        return res;
    };

    const std::shared_ptr<AggregatorNode>
    Aggregator::findNextOfBestMatchForRead(const Slice & target, RWLockReadGuard * guard,
                                           std::string * lower_bound) const {
        std::shared_ptr<AggregatorNode> res;
        std::string bound;
        load:
        {
            RWLockReadGuard dispatcher_guard(_dispatcher_lock);
            auto find_res = _dispatcher.upper_bound(target);
            if (find_res == _dispatcher.begin()) {
            } else { --find_res; }

            ++find_res;
            if (find_res == _dispatcher.end()) { return nullptr; }
            res = find_res->second;

            if (lower_bound != nullptr) { bound = find_res->first; }
        }

        RWLockReadGuard temp_guard(res->lock);
        if (res->dirty) { goto load; }
        *guard = std::move(temp_guard);
        if (lower_bound != nullptr) { *lower_bound = std::move(bound); }
        return res;
    };

    void Aggregator::mayOpenDB(std::shared_ptr<AggregatorNode> match) {
        if (match->db == nullptr) {
            match->db = std::make_unique<DBSingle>(match->db_name, Options{}, &_seq_gen);
            ++_operating_dbs;
        }
    };

    std::unique_ptr<DB>
    Aggregator::mayRenameDB(std::unique_ptr<DB> && db) {
        if (db->immut_name().empty()) {
            return std::move(db);
        }
        assert(db->immut_name().find('+') != std::string::npos ||
               db->immut_name().find('_') != std::string::npos);
        std::string name = std::move(db->mut_name());
        std::string after_name;
        db = nullptr;

        uint64_t cnt;
        {
            std::lock_guard<std::mutex> guard(_mutex);
            cnt = _meta->immut_value().counter;
            _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
        }

        Logger::logForMan(_logger.get(), "rename %s to %llu", name.c_str(), static_cast<unsigned long long>(cnt));
        IOEnv::renameFile(name, after_name = ((_name + '/') += std::to_string(cnt)));
        return std::make_unique<DBSingle>(std::move(after_name), Options{}, &_seq_gen);
    };

    void Aggregator::ifCompact1To2Done(std::shared_ptr<AggregatorNode> match) {
        if (match->db->immut_name().empty() && match->db->canRelease()) {
            assert(match->db_name.empty());
            auto * compact_db = static_cast<Compacting1To2DB *>(match->db.get());
            compact_db->syncFiles();
            std::unique_ptr<DB> product_a = std::move(compact_db->mut_product_a());
            std::unique_ptr<DB> product_b = std::move(compact_db->mut_product_b());
            match->dirty = true;

            auto node = std::make_shared<AggregatorNode>();
            node->db = mayRenameDB(std::move(product_a));
            node->db_name = node->db->immut_name();
            node->hit = match->hit / 2;

            auto next_node = std::make_shared<AggregatorNode>();
            next_node->db = mayRenameDB(std::move(product_b));
            next_node->db_name = next_node->db->immut_name();
            next_node->hit.store(node->hit);

            {
                RWLockWriteGuard dispatcher_guard(_dispatcher_lock);
                auto find_res = --_dispatcher.upper_bound(node->db->smallestKey());
                assert(find_res->second == match);
                find_res->second = node;
                _dispatcher[next_node->db->smallestKey().toString()] = next_node;
            }
            ++_operating_dbs;

            Logger::logForMan(_logger.get(), "compacting db to %s, %s",
                              node->db_name.c_str(),
                              next_node->db_name.c_str());
        }
    };

    void Aggregator::gc() {
        std::vector<unsigned> hit_q;

        {
            std::shared_ptr<AggregatorNode> cursor;
            std::shared_ptr<AggregatorNode> next;
            RWLockWriteGuard cursor_guard;
            RWLockWriteGuard next_guard;
            std::string cursor_bound;
            std::string next_bound;

            cursor = findBestMatchForWrite(Slice(), &cursor_guard, &cursor_bound);
            while (true) {
                if (cursor->db != nullptr) {
                    hit_q.emplace_back(cursor->hit);
                }

                next = findNextOfBestMatchForWrite(cursor_bound, &next_guard, &next_bound);
                if (next == nullptr) {
                    break;
                }

                if (!cursor->dirty && !next->dirty
                    && cursor->db != nullptr && next->db != nullptr
                    && !cursor->db->immut_name().empty() && !next->db->immut_name().empty()
                    && cursor->db->canRelease() && next->db->canRelease()
                    && static_cast<DBSingle *>(cursor->db.get())->spaceUsage() +
                       static_cast<DBSingle *>(next->db.get())->spaceUsage() < AggregatorConst::merge_threshold_) {

                    std::unique_ptr<DB> product;
                    {
                        Compacting2To1Worker worker(std::move(cursor->db), std::move(next->db), &_seq_gen);
                        product = std::move(worker.mut_product());
                    }
                    cursor->dirty = true;
                    next->dirty = true;

                    for (const std::string * del_name:{&cursor->db_name, &next->db_name}) {
                        for (const std::string & c:IOEnv::getChildren(*del_name)) {
                            IOEnv::deleteFile((*del_name + '/') += c);
                        }
                        IOEnv::deleteDir(*del_name);
                    }

                    auto node = std::make_shared<AggregatorNode>();
                    node->db = mayRenameDB(std::move(product));
                    node->db_name = node->db->immut_name();
                    node->hit = cursor->hit + next->hit;

                    {
                        RWLockWriteGuard guard(_dispatcher_lock);
                        auto find_res = --_dispatcher.upper_bound(cursor_bound);
                        assert(find_res->second == cursor);
                        find_res->second = node;

                        find_res = --_dispatcher.upper_bound(next_bound);
                        assert(find_res->second == next);
                        _dispatcher.erase(find_res);
                    }
                    --_operating_dbs;

                    Logger::logForMan(_logger.get(), "%s, %s to %s",
                                      cursor->db_name.c_str(),
                                      next->db_name.c_str(),
                                      node->db_name.c_str());
                }

                { // release lock of cursor
                    RWLockWriteGuard _(std::move(cursor_guard));
                }
                cursor = std::move(next); // could pass dirty node, it's fine for iter
                cursor_guard = std::move(next_guard);
                cursor_bound = std::move(next_bound);
            }
        }

        size_t curr_dbs = hit_q.size();
        static_assert(AggregatorConst::max_dbs_ > 1, "nonsense setting");
        if (curr_dbs < AggregatorConst::max_dbs_) {
        } else {
            std::sort(hit_q.begin(), hit_q.end());
            size_t close_limit = hit_q[curr_dbs - AggregatorConst::max_dbs_] / 2 * 3;

            std::shared_ptr<AggregatorNode> cursor;
            RWLockWriteGuard cursor_guard;
            std::string cursor_bound;

            cursor = findBestMatchForWrite(Slice(), &cursor_guard, &cursor_bound);
            while (cursor != nullptr) {
                if (cursor->db != nullptr && (cursor->db->tryApplyPending(), cursor->db->canRelease())
                    && cursor->hit <= close_limit) {
                    cursor->db = nullptr;
                    --_operating_dbs;
                }
                cursor->hit = 0;
                cursor = findNextOfBestMatchForWrite(cursor_bound, &cursor_guard, &cursor_bound);
            }
        }
    }

    bool repairDB(const std::string & db_name, reporter_t reporter) noexcept {
        static constexpr char postfix[] = "tmp";
        try {
            uint64_t max_num = 0;
            for (const std::string & child:IOEnv::getChildren(db_name)) {
                if (child[0] >= '0' && child[0] <= '9') { // find db
                    std::string prefixed_child = (db_name + '/') += child;
                    if (child.size() > sizeof(postfix) &&
                        std::equal(postfix, postfix + sizeof(postfix),
                                   child.cend() - sizeof(postfix), child.cend())) { // temp files
                        for (const std::string & c:IOEnv::getChildren(prefixed_child)) {
                            IOEnv::deleteFile((prefixed_child + '/') += c);
                        }
                        IOEnv::deleteDir(prefixed_child);
                    } else {
                        max_num = std::max<unsigned long long>(max_num, std::stoull(child));
                        if (!IOEnv::fileExists(prefixed_child + "/keeper") &&
                            !repairDBSingle(prefixed_child, reporter)) {
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