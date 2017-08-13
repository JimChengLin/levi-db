#ifndef LEVIDB_SEQ_GEN_H
#define LEVIDB_SEQ_GEN_H

/*
 * 生成全局唯一序列号并管理快照
 */

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>

#include "util.h"

namespace LeviDB {
    /*
     * doubly-linked list
     *    |--------------------|
     *   old <-> new <-> d-head|
     */
    class Snapshot {
    private:
        Snapshot * _prev;
        Snapshot * _next;
        const uint64_t _seq_num = 0;

        friend class SeqGenerator;

    public:
        Snapshot() noexcept : _prev(this), _next(this) {}

        Snapshot(uint64_t seq_num, Snapshot * dummy_head) noexcept;

        ~Snapshot() noexcept;

        uint64_t immut_seq_num() const noexcept { return _seq_num; }

        DELETE_MOVE(Snapshot);
        DELETE_COPY(Snapshot);
    };

    class SeqGenerator {
    private:
        Snapshot _dummy_head;
        std::atomic<std::uint64_t> _seq{0};

    public:
        ~SeqGenerator() noexcept = default;
        DELETE_MOVE(SeqGenerator);
        DELETE_COPY(SeqGenerator);

        uint64_t uniqueSeqAtomic() noexcept { return _seq.fetch_add(1); }

        std::unique_ptr<Snapshot> makeSnapshot() noexcept;

        bool empty() const noexcept { return _dummy_head._next == &_dummy_head; }

        uint64_t oldest() const noexcept;

        uint64_t newest() const noexcept;
    };
}

#endif //LEVIDB_SEQ_GEN_H