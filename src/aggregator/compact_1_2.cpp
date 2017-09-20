#include <thread>

#include "compact_1_2.h"

namespace LeviDB {
    static uint32_t compress_block_size_ = 4096;

    template<bool FRONT = true>
    void Task(Iterator<Slice, std::string> * it,
              DB * target,
              const Slice & end_at,
              std::exception_ptr & e_ptr,
              const std::function<bool(const Slice &)> & ignore_check) noexcept {
        try {
            WriteOptions options{};
            if (!target->immut_options().compression) {
                for (FRONT ? it->seekToFirst() : it->seekToLast();
                     it->valid();
                     FRONT ? it->next() : it->prev()) {

                    if (ignore_check(it->key())) {
                        if (it->key() == end_at) { break; }
                        continue;
                    }

                    target->put(options, it->key(), it->value());
                    if (it->key() == end_at) { break; }
                }
            } else {
                options.compress = true;
                std::vector<std::pair<std::string, std::string>> q;
                std::vector<std::pair<Slice, Slice>> slice_q;
                for (FRONT ? it->seekToFirst() : it->seekToLast();
                     it->valid();
                     FRONT ? it->next() : it->prev()) {

                    if (ignore_check(it->key())) {
                        if (it->key() == end_at) { break; }
                        continue;
                    }

                    q.emplace_back(it->key().toString(), it->value());
                    options.uncompress_size += q.back().first.size() + q.back().second.size();
                    if (options.uncompress_size >= compress_block_size_ || it->key() == end_at) {
                        for (auto const & kv:q) {
                            slice_q.emplace_back(kv.first, kv.second);
                        }
                        target->write(options, slice_q);

                        q.clear();
                        slice_q.clear();
                        options.uncompress_size = 0;
                    }
                    if (it->key() == end_at) { break; }
                }
            }
            target->put(options, {}, {}); // sync
        } catch (const Exception & e) {
            e_ptr = std::current_exception();
        }
    }

    Compacting1To2DB::Compacting1To2DB(std::unique_ptr<DB> && resource, SeqGenerator * seq_gen) noexcept
            : _resource(std::move(resource)),
              _product_a(std::make_unique<DBSingle>(_resource->immut_name() + "_a",
                                                    _resource->immut_options(), seq_gen)),
              _product_b(std::make_unique<DBSingle>(_resource->immut_name() + "_b",
                                                    _resource->immut_options(), seq_gen)) {
        // total size
        auto it = _resource->makeIterator(seq_gen->makeSnapshot());
        uint32_t size = 0;
        for (it->seekToFirst();
             it->valid();
             it->next()) { ++size; }
        // fail if there is a single KV that occupies 4GB disk space, but it's extremely rare.
        uint32_t mid = size / 2 - 1;
        it->seekToFirst();
        for (uint32_t i = 0; i < mid; ++i) { it->next(); }
        // end point
        _a_end = it->key().toString();
        it->next();
        _b_end = it->key().toString();

        // spawn tasks
        std::thread front_task([iter = std::move(it), this]() noexcept {
            Task<true>(iter.get(), _product_a.get(), _a_end, _e_a, [&](const Slice & k) noexcept {
                return isKeyIgnored(k);
            });
            {
                std::lock_guard<std::mutex> guard(_lock);
                _a_end.clear();
                if (_a_end.empty() && _b_end.empty()) {
                    _compacting = false;
                }
            }
        });
        std::thread back_task([iter = _resource->makeIterator(seq_gen->makeSnapshot()), this]() noexcept {
            Task<false>(iter.get(), _product_b.get(), _b_end, _e_b, [&](const Slice & k) noexcept {
                return isKeyIgnored(k);
            });
            {
                std::lock_guard<std::mutex> guard(_lock);
                _b_end.clear();
                if (_a_end.empty() && _b_end.empty()) {
                    _compacting = false;
                }
            }
        });
        front_task.detach();
        back_task.detach();
    }

    Compacting1To2DB::~Compacting1To2DB() noexcept { assert(!_compacting); }

    bool Compacting1To2DB::put(const WriteOptions & options, const Slice & key, const Slice & value) {

    }

    bool Compacting1To2DB::remove(const WriteOptions & options, const Slice & key) {

    }

    // batch write may cause compaction not perfectly dividing db into two equal parts
    // but the effect should be really trivial
    bool Compacting1To2DB::write(const WriteOptions & options, const std::vector<std::pair<Slice, Slice>> & kvs) {

    }

    std::pair<std::string, bool>
    Compacting1To2DB::get(const ReadOptions & options, const Slice & key) const {

    }

    std::unique_ptr<Snapshot>
    Compacting1To2DB::makeSnapshot() {

    }

    std::unique_ptr<Iterator<Slice, std::string>>
    Compacting1To2DB::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {
    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                        std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Compacting1To2DB::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                                std::unique_ptr<Snapshot> && snapshot) const {
    };

    void Compacting1To2DB::tryApplyPending() {
    };

    bool Compacting1To2DB::canRelease() const {
    };

    void Compacting1To2DB::reportIgnoredKey(const Slice & k) noexcept {
    };

    bool Compacting1To2DB::isKeyIgnored(const Slice & k) const noexcept {
    };
}