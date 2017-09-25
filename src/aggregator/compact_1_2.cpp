#ifndef __clang__
#include <algorithm>
#endif

#include "../log_reader.h"
#include "compact_1_2.h"

namespace LeviDB {
    static constexpr uint32_t page_size_ = 4096;

    template<bool FRONT = true>
    void Task(Iterator<Slice, std::string> * it,
              std::unique_ptr<DB> & target,
              const Slice & end_at,
              std::exception_ptr & e_ptr,
              std::atomic<bool> & e_bool,
              const std::set<std::string, SliceComparator> & ignore,
              ReadWriteLock & ignore_lock,
              ReadWriteLock & target_lock,
              SeqGenerator * seq_gen) noexcept {
        try {
            WriteOptions options{};
            if (!target->immut_options().compression) {
                for (FRONT ? it->seekToFirst() : it->seekToLast();
                     it->valid();
                     FRONT ? it->next() : it->prev()) {
                    {
                        RWLockReadGuard read_guard(ignore_lock);
                        if (ignore.find(it->key()) != ignore.end()) {
                        } else {
                            // coverity[double_lock]
                            RWLockWriteGuard write_guard(target_lock);
                            if (!target->put(options, it->key(), it->value())) {
                                target = std::make_unique<Compacting1To2DB>(std::move(target), seq_gen);
                                target->put(options, it->key(), it->value());
                            };
                        }
                    }
                    if (it->key() == end_at) { break; }
                }
            } else {
                options.compress = true;
                std::vector<std::pair<std::string, std::string>> q;
                std::vector<std::pair<Slice, Slice>> slice_q;
                for (FRONT ? it->seekToFirst() : it->seekToLast();
                     it->valid();
                     FRONT ? it->next() : it->prev()) {

                    q.emplace_back(it->key().toString(), it->value());
                    options.uncompress_size += q.back().first.size() + q.back().second.size();
                    if (options.uncompress_size >= page_size_ || it->key() == end_at) {

                        if (FRONT) {
                            for (auto const & kv:q) {
                                slice_q.emplace_back(kv.first, kv.second);
                            }
                        } else {
                            for (auto cbegin = q.crbegin(); cbegin != q.crend(); ++cbegin) {
                                slice_q.emplace_back(cbegin->first, cbegin->second);
                            }
                        }

                        {
                            RWLockReadGuard read_guard(ignore_lock);
                            slice_q.erase(std::remove_if(slice_q.begin(), slice_q.end(),
                                                         [&ignore](const std::pair<Slice, Slice> & k) noexcept {
                                                             return ignore.find(k.first) != ignore.end();
                                                         }), slice_q.end());
                            if (!slice_q.empty()) {
                                // coverity[double_lock]
                                RWLockWriteGuard write_guard(target_lock);
                                if (!target->write(options, slice_q)) {
                                    target = std::make_unique<Compacting1To2DB>(std::move(target), seq_gen);
                                    target->write(options, slice_q);
                                };
                            }
                        }

                        q.clear();
                        slice_q.clear();
                        options.uncompress_size = 0;
                    }
                    if (it->key() == end_at) { break; }
                }
            }
            {
                RWLockWriteGuard write_guard(target_lock);
                target->put(WriteOptions{}, {}, {});
            }
        } catch (const Exception & e) {
            e_ptr = std::current_exception();
            e_bool = true;
        }
    }

    Compacting1To2DB::Compacting1To2DB(std::unique_ptr<DB> && resource, SeqGenerator * seq_gen)
            : _seq_gen(seq_gen),
              _resource(std::move(resource)),
              _product_a(std::make_unique<DBSingle>(_resource->immut_name() + "_a",
                                                    _resource->immut_options().createIfMissing(true), seq_gen)),
              _product_b(std::make_unique<DBSingle>(_resource->immut_name() + "_b",
                                                    _resource->immut_options().createIfMissing(true), seq_gen)) {
        // total size
        auto it = _resource->makeIterator(std::make_unique<Snapshot>(UINT64_MAX));
        uint32_t size = 0;
        for (it->seekToFirst();
             it->valid();
             it->next()) { ++size; }
        // fail if there is a single KV that occupies 4GB disk space, but it's extremely impossible.
        uint32_t mid = size / 2 - 1;
        it->seekToFirst();
        for (uint32_t i = 0; i < mid; ++i) { it->next(); }
        // end point
        _a_end = it->key().toString();
        it->next();
        _b_end = it->key().toString();

        _compacting = true;
        // spawn threads
        std::thread front_task([iter = std::move(it), this]() noexcept {
            Task<true>(iter.get(), _product_a, _a_end, _e_a, _e_a_bool, _ignore, _rw_lock, _a_lock, _seq_gen);
            _a_end_meet = true;
            if (_b_end_meet) {
                _compacting = false;
                try {
                    RWLockWriteGuard write_guard(_rw_lock);
                    _ignore.clear();
                } catch (const Exception & e) {}
            }
        });
        std::thread back_task(
                [iter = _resource->makeIterator(std::make_unique<Snapshot>(UINT64_MAX)), this]() noexcept {
                    Task<false>(iter.get(), _product_b, _b_end, _e_b, _e_b_bool, _ignore, _rw_lock, _b_lock, _seq_gen);
                    _b_end_meet = true;
                    if (_a_end_meet) {
                        _compacting = false;
                        try {
                            RWLockWriteGuard write_guard(_rw_lock);
                            _ignore.clear();
                        } catch (const Exception & e) {}
                    }
                });
        front_task.detach();
        back_task.detach();
    }

    Compacting1To2DB::~Compacting1To2DB() noexcept { assert(canRelease()); }

    bool Compacting1To2DB::put(const WriteOptions & options, const Slice & key, const Slice & value) {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        if (_compacting) {
            RWLockWriteGuard write_guard(_rw_lock);
            _ignore.emplace(key.toString());
        }
        {
            RWLockWriteGuard write_guard(_a_lock);
            if ((_product_a->largestKey().size() != 0 && !SliceComparator{}(_product_a->largestKey(), key))
                || (_compacting && !SliceComparator{}(_a_end, key))) {
                if (!_product_a->put(options, key, value)) {
                    _product_a = std::make_unique<Compacting1To2DB>(std::move(_product_a), _seq_gen);
                    return _product_a->put(options, key, value);
                }
                return true;
            }
        }
        {
            RWLockWriteGuard write_guard(_b_lock);
            if (!_product_b->put(options, key, value)) {
                _product_b = std::make_unique<Compacting1To2DB>(std::move(_product_b), _seq_gen);
                return _product_b->put(options, key, value);
            }
            return true;
        }
    }

    bool Compacting1To2DB::remove(const WriteOptions & options, const Slice & key) {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        if (_compacting) {
            RWLockWriteGuard write_guard(_rw_lock);
            _ignore.emplace(key.toString());
        }
        {
            RWLockWriteGuard write_guard(_a_lock);
            if ((_product_a->largestKey().size() != 0 && !SliceComparator{}(_product_a->largestKey(), key))
                || (_compacting && !SliceComparator{}(_a_end, key))) {
                if (!_product_a->remove(options, key)) {
                    _product_a = std::make_unique<Compacting1To2DB>(std::move(_product_a), _seq_gen);
                    return _product_a->remove(options, key);
                };
                return true;
            }
        }
        {
            RWLockWriteGuard write_guard(_b_lock);
            if (!_product_b->remove(options, key)) {
                _product_b = std::make_unique<Compacting1To2DB>(std::move(_product_b), _seq_gen);
                return _product_b->remove(options, key);
            }
            return true;
        }
    }

    bool Compacting1To2DB::write(const WriteOptions & options, const std::vector<std::pair<Slice, Slice>> & kvs) {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        if (_compacting) {
            RWLockWriteGuard write_guard(_rw_lock);
            for (const auto & kv:kvs) {
                _ignore.emplace(kv.first.toString());
            }
        }

        const Slice & head = kvs.front().first;
        const Slice & tail = kvs.back().first;
        {
            RWLockWriteGuard a_write_guard(_a_lock);
            bool head_in_part_a =
                    ((_product_a->largestKey().size() != 0 && !SliceComparator{}(_product_a->largestKey(), head))
                     || (_compacting && !SliceComparator{}(_a_end, head)));
            if (head_in_part_a) {
                if (!_product_a->write(options, kvs)) {
                    _product_a = std::make_unique<Compacting1To2DB>(std::move(_product_a), _seq_gen);
                    _product_a->write(options, kvs); // after making CompactingDB, it should return true anyway
                };
            } else { // not in part a
                RWLockWriteGuard _(std::move(a_write_guard));
            }

            RWLockWriteGuard b_write_guard(_b_lock);
            bool tail_in_part_b =
                    ((_product_b->smallestKey().size() != 0 && !SliceComparator{}(tail, _product_b->smallestKey()))
                     || (_compacting && !SliceComparator{}(tail, _b_end)));
            if (!tail_in_part_b && head_in_part_a) { // all in part a
                return true;
            }

            if (!head_in_part_a) { // kvs are in the middle / all in part b
                if (!_product_b->write(options, kvs)) {
                    _product_b = std::make_unique<Compacting1To2DB>(std::move(_product_b), _seq_gen);
                    return _product_b->write(options, kvs);
                }
                return true;
            }
            // 交叉 case
            assert(head_in_part_a && tail_in_part_b);
            std::vector<std::pair<Slice, Slice>> tail_part;
            bool meet_first = false;
            for (const auto & kv:kvs) {
                if (meet_first || (meet_first = ((_product_b->smallestKey().size() != 0 &&
                                                  !SliceComparator{}(kv.first, _product_b->smallestKey()))
                                                 || (_compacting && !SliceComparator{}(kv.first, _b_end))))) {
                    tail_part.emplace_back(kv.first, kv.second);
                }
            }

            if (!_product_b->write(options, tail_part)) {
                _product_b = std::make_unique<Compacting1To2DB>(std::move(_product_b), _seq_gen);
                return _product_b->write(options, tail_part);
            }
            for (const auto & kv:tail_part) {
                if (!_product_a->remove(options, kv.first)) {
                    _product_a = std::make_unique<Compacting1To2DB>(std::move(_product_a), _seq_gen);
                    _product_a->remove(options, kv.first);
                };
            }

            _product_a->updateKeyRange();
            _product_b->updateKeyRange();
            return true;
        }
    }

    std::pair<std::string, bool>
    Compacting1To2DB::get(const ReadOptions & options, const Slice & key) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        if (_compacting) {
            RWLockReadGuard read_guard(_rw_lock);
            if (_ignore.find(key) == _ignore.end()) {
                return _resource->get(options, key);
            }
        }
        {
            RWLockReadGuard read_guard(_a_lock);
            assert(_product_a->largestKey().size() != 0);
            if (!SliceComparator{}(_product_a->largestKey(), key)) {
                return _product_a->get(options, key);
            }
        }
        {
            RWLockReadGuard read_guard(_b_lock);
            return _product_b->get(options, key);
        }
    }

    std::unique_ptr<Snapshot>
    Compacting1To2DB::makeSnapshot() {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        return _seq_gen->makeSnapshot();
    }

    std::unique_ptr<Iterator<Slice, std::string>>
    Compacting1To2DB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                                std::unique_ptr<Snapshot> && snapshot) const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
    };

    void Compacting1To2DB::tryApplyPending() {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        _resource->tryApplyPending();
        {
            RWLockWriteGuard write_guard(_a_lock);
            _product_a->tryApplyPending();
        }
        {
            RWLockWriteGuard write_guard(_b_lock);
            _product_b->tryApplyPending();
        }
    };

    bool Compacting1To2DB::canRelease() const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        RWLockReadGuard a_read_guard(_a_lock);
        RWLockReadGuard b_read_guard(_b_lock);
        return !_compacting && _resource->canRelease() && _product_a->canRelease() && _product_b->canRelease();
    };

    Slice Compacting1To2DB::largestKey() const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        RWLockReadGuard read_guard(_b_lock);
        if (_product_b->largestKey().size() != 0) {
            if (!_compacting) {
                return _product_b->largestKey();
            }
            return std::max(_product_b->largestKey(), _resource->largestKey(), SliceComparator{});
        }
        return _resource->largestKey();
    };

    Slice Compacting1To2DB::smallestKey() const {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        RWLockReadGuard read_guard(_a_lock);
        if (_product_a->smallestKey().size() != 0) {
            if (!_compacting) {
                return _product_a->smallestKey();
            }
            return std::min(_product_a->smallestKey(), _resource->smallestKey(), SliceComparator{});
        }
        return _resource->smallestKey();
    };

    void Compacting1To2DB::updateKeyRange() {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }
        {
            RWLockWriteGuard write_guard(_a_lock);
            _product_a->updateKeyRange();
        }
        {
            RWLockWriteGuard write_guard(_b_lock);
            _product_b->updateKeyRange();
        }
    }

    bool Compacting1To2DB::explicitRemove(const WriteOptions & options,
                                          const Slice & key) {
        if (_e_a_bool) { std::rethrow_exception(_e_a); }
        if (_e_b_bool) { std::rethrow_exception(_e_b); }

        if (_compacting) {
            RWLockWriteGuard write_guard(_rw_lock);
            _ignore.emplace(key.toString());
        }
        {
            RWLockWriteGuard write_guard(_a_lock);
            if ((_product_a->largestKey().size() != 0 && !SliceComparator{}(_product_a->largestKey(), key))
                || (_compacting && !SliceComparator{}(_a_end, key))) {
                if (!_product_a->explicitRemove(options, key)) {
                    _product_a = std::make_unique<Compacting1To2DB>(std::move(_product_a), _seq_gen);
                    return _product_a->explicitRemove(options, key);
                };
                return true;
            }
        }
        {
            RWLockWriteGuard write_guard(_b_lock);
            if (!_product_b->explicitRemove(options, key)) {
                _product_b = std::make_unique<Compacting1To2DB>(std::move(_product_b), _seq_gen);
                return _product_b->explicitRemove(options, key);
            }
            return true;
        }
    }

    bool repairCompacting1To2DB(const std::string & db_name, reporter_t reporter) noexcept {
        try {
            std::string product_a_name = db_name + "_a";
            std::string product_a_temp = product_a_name + "_temp";
            IOEnv::renameFile(product_a_name, product_a_temp);

            std::string product_b_name = db_name + "_b";
            std::string product_b_temp = product_b_name + "_temp";
            IOEnv::renameFile(product_b_name, product_b_temp);

            SeqGenerator seq_gen;
            Compacting1To2DB db(std::make_unique<DBSingle>(db_name, Options{}, &seq_gen), &seq_gen);

            // moving values from product_a_temp and product_b_temp to db
            for (const std::string * name:{&product_a_temp, &product_b_temp}) {
                RandomAccessFile rf(*name + "/data");
                auto it = LogReader::makeTableRecoveryIteratorKV(&rf, reporter);

                WriteOptions write_opt{};
                write_opt.compress = true;
                std::map<std::string, std::string, SliceComparator> q;
                std::vector<std::pair<Slice, Slice>> slice_q;

                while (it->valid()) {
                    auto item = it->item();
                    if (item.second.back() == 1) { // del
                        db.remove(WriteOptions{}, item.first);
                        auto find_res = q.find(item.first);
                        if (find_res != q.end()) { q.erase(find_res); }
                    } else {
                        item.second.pop_back();
                        write_opt.uncompress_size += item.first.size() + item.second.size();
                        q[item.first.toString()] = std::move(item.second);
                    }
                    it->next();

                    if (write_opt.uncompress_size >= page_size_ || !it->valid()) {
                        for (auto const & kv:q) {
                            slice_q.emplace_back(kv.first, kv.second);
                        }
                        db.write(write_opt, slice_q);

                        q.clear();
                        slice_q.clear();
                        write_opt.uncompress_size = 0;
                    }
                }
            }

            while (db.immut_compacting()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            // delete temp dirs
            for (const std::string * name:{&product_a_temp, &product_b_temp}) {
                for (const std::string & child:LeviDB::IOEnv::getChildren(*name)) {
                    LeviDB::IOEnv::deleteFile((*name + '/') += child);
                }
                LeviDB::IOEnv::deleteDir(*name);
            }
        } catch (const Exception & e) {
            reporter(e);
            return false;
        }
        return true;
    }
}