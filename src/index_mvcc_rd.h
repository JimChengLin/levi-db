#ifndef LEVIDB_INDEX_MVCC_H
#define LEVIDB_INDEX_MVCC_H

/*
 * 继承原有索引, 添加 MVCC 支持(接口), 实现真正的 Matcher
 */

#include <deque>
#include <map>

#include "index.h"
#include "seq_gen.h"

namespace LeviDB {
    class IndexMVCC : private BitDegradeTree {
    private:
        typedef std::pair<uint64_t/* seq_num */, std::pair<std::string/* k */, OffsetToData/* v */>> bundle;

        SeqGenerator * _seq_gen;
        std::deque<bundle> _pending;

    public:
        IndexMVCC(const std::string & fname, SeqGenerator * seq_gen)
                : BitDegradeTree(fname), _seq_gen(seq_gen) {};

        IndexMVCC(const std::string & fname, OffsetToEmpty empty, SeqGenerator * seq_gen)
                : BitDegradeTree(fname, empty), _seq_gen(seq_gen) {};

        // 返回最接近 k 的结果, 参见 base
        OffsetToData find(const Slice & k, uint64_t seq_num) const;

        size_t size(uint64_t seq_num) const;

        size_t size(const BDNode * node, uint64_t seq_num) const;

        // 以下方法的调用者必须有读写锁保护且快照只读
        // 无需显式提供 seq_num
        void insert(const Slice & k, OffsetToData v);

        void remove(const Slice & k);

        void sync();

    private:
        std::map<std::string/* k */, OffsetToData/* v */>
        pendingPart() const noexcept;

        void applyPending();
    };
}

#endif //LEVIDB_INDEX_MVCC_H