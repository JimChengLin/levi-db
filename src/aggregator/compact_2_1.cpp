#include "compact_2_1.h"

namespace LeviDB {
    static constexpr uint32_t page_size_ = 4096;

    void Task(Iterator<Slice, std::string> * it,
              DB * target,
              std::exception_ptr & e_ptr) noexcept {
        try {
            WriteOptions options{};
            if (!target->immut_options().compression) {
                for (it->seekToFirst();
                     it->valid();
                     it->next()) {
                    target->put(options, it->key(), it->value());
                }
            } else {
                options.compress = true;
                std::vector<std::pair<std::string, std::string>> q;
                std::vector<std::pair<Slice, Slice>> slice_q;
                for (it->seekToFirst(); it->valid();) {
                    q.emplace_back(it->key().toString(), it->value());
                    options.uncompress_size += q.back().first.size() + q.back().second.size();
                    it->next();

                    if (options.uncompress_size >= page_size_ || !it->valid()) {
                        for (auto const & kv:q) {
                            slice_q.emplace_back(kv.first, kv.second);
                        }
                        if (!slice_q.empty()) {
                            target->write(options, slice_q);
                        }
                        q.clear();
                        slice_q.clear();
                        options.uncompress_size = 0;
                    }
                }
            }
        } catch (const Exception & e) {
            e_ptr = std::current_exception();
        }
    }

    Compacting2To1Worker::Compacting2To1Worker(std::unique_ptr<DB> && resource_a, std::unique_ptr<DB> && resource_b,
                                               SeqGenerator * seq_gen)
            : _resource_a(std::move(resource_a)),
              _resource_b(std::move(resource_b)),
              _product(std::make_unique<DBSingle>(_resource_a->immut_name() + '+' +
                                                  _resource_b->immut_name().substr(
                                                          _resource_b->immut_name().rfind('/') + 1),
                                                  _resource_a->immut_options()
                                                          .createIfMissing(true).errorIfExists(true), seq_gen)) {
        assert(_resource_a->canRelease() && _resource_b->canRelease());

        std::thread task([iter = _resource_a->makeIterator(std::make_unique<Snapshot>(UINT64_MAX)), this]() noexcept {
            Task(iter.get(), _product.get(), _e_a);
        });
        std::thread task_2([iter = _resource_b->makeIterator(std::make_unique<Snapshot>(UINT64_MAX)), this]() noexcept {
            Task(iter.get(), _product.get(), _e_b);
        });
        task.join();
        task_2.join();

        for (const auto & e:{_e_a, _e_b}) {
            if (e != nullptr) {
                std::rethrow_exception(e);
            }
        }

        std::string old_name = std::move(_product->mut_name());
        std::string new_name = old_name + '-';
        _product->sync();
        _product = nullptr;
        IOEnv::renameFile(old_name, new_name);
        _product = std::make_unique<DBSingle>(std::move(new_name),
                                              _resource_a->immut_options().createIfMissing(false).errorIfExists(false),
                                              seq_gen);
    }
}