#include <cassert>

#include "seq_gen.h"

namespace LeviDB {
    thread_local static bool fraud = false;

    Snapshot::Snapshot(uint64_t seq_num, Snapshot * dummy_head) noexcept: _seq_num(seq_num) {
        _next = dummy_head;
        _prev = dummy_head->_prev;
        _prev->_next = this;
        _next->_prev = this;
    }

    Snapshot::~Snapshot() noexcept {
        _prev->_next = _next;
        _next->_prev = _prev;
    }

    std::unique_ptr<Snapshot> SeqGenerator::makeSnapshot() noexcept {
        return std::make_unique<Snapshot>(uniqueSeqAtomic(), &_dummy_head);
    }

    bool SeqGenerator::empty() const noexcept {
        return fraud || _dummy_head._next == &_dummy_head;
    }

    uint64_t SeqGenerator::oldest() const noexcept {
        assert(!empty());
        return _dummy_head._next->immut_seq_num();
    }

    uint64_t SeqGenerator::newest() const noexcept {
        assert(!empty());
        return _dummy_head._prev->immut_seq_num();
    }

    void stashCurrSeqGen() noexcept { fraud = true; };

    void stashPopCurrSeqGen() noexcept { fraud = false; };
}