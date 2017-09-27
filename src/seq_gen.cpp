#include <cassert>
#include <mutex>

#include "seq_gen.h"

namespace LeviDB {
    static std::mutex _lock;
    thread_local static bool _fraud = false;

    Snapshot::Snapshot(uint64_t seq_num, Snapshot * dummy_head) noexcept: _seq_num(seq_num) {
        _next = dummy_head;
        _prev = dummy_head->_prev;
        _prev->_next = this;
        _next->_prev = this;
    }

    Snapshot::~Snapshot() noexcept {
        std::lock_guard<std::mutex> guard(_lock);
        _prev->_next = _next;
        _next->_prev = _prev;
    }

    std::unique_ptr<Snapshot> SeqGenerator::makeSnapshot() noexcept {
        std::lock_guard<std::mutex> guard(_lock);
        return std::make_unique<Snapshot>(uniqueSeqAtomic(), &_dummy_head);
    }

    bool SeqGenerator::empty() const noexcept {
        std::lock_guard<std::mutex> guard(_lock);
        return _dummy_head._next == &_dummy_head;
    }

    uint64_t SeqGenerator::oldest() const noexcept {
        assert(!empty());
        std::lock_guard<std::mutex> guard(_lock);
        return _dummy_head._next->immut_seq_num();
    }

    uint64_t SeqGenerator::newest() const noexcept {
        assert(!empty());
        std::lock_guard<std::mutex> guard(_lock);
        return _dummy_head._prev->immut_seq_num();
    }

    void stashCurrSeqGen() noexcept { _fraud = true; };

    void stashPopCurrSeqGen() noexcept { _fraud = false; };

    bool isFraudMode() noexcept { return _fraud; };
}