#ifndef LEVIDB_SEQ_GEN_H
#define LEVIDB_SEQ_GEN_H

/*
 * 生成全局唯一序列号并管理快照
 * 编号从 1 开始
 */

#include <atomic>
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

        // dummy copy, lifetime is not bound by SeqGenerator
        explicit Snapshot(uint64_t seq_num) noexcept : _prev(this), _next(this), _seq_num(seq_num) {}

        Snapshot(uint64_t seq_num, Snapshot * dummy_head) noexcept;

        ~Snapshot() noexcept;

        uint64_t immut_seq_num() const noexcept { return _seq_num; }

        DELETE_MOVE(Snapshot);
        DELETE_COPY(Snapshot);
    };

    class SeqGenerator {
    private:
        Snapshot _dummy_head;
        std::atomic<std::uint64_t> _seq{1};

    public:
        SeqGenerator() noexcept = default;

        ~SeqGenerator() noexcept = default;

        DELETE_MOVE(SeqGenerator);
        DELETE_COPY(SeqGenerator);

        uint64_t uniqueSeqAtomic() noexcept { return _seq.fetch_add(1); }

        std::unique_ptr<Snapshot> makeSnapshot() noexcept;

        bool empty() const noexcept;

        uint64_t oldest() const noexcept;

        uint64_t newest() const noexcept;
    };

    // dirty hack, 让 SeqGenerator 暂时进入欺骗模式, 效果类似于永远 empty
    void stashCurrSeqGen() noexcept;

    // 恢复 SeqGenerator
    void stashPopCurrSeqGen() noexcept;

    bool isFraudMode() noexcept;
}

#endif //LEVIDB_SEQ_GEN_H