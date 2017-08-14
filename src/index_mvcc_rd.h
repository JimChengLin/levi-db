#ifndef LEVIDB_INDEX_MVCC_H
#define LEVIDB_INDEX_MVCC_H

/*
 * 继承原有索引, 添加 MVCC 支持(接口), 实现真正的 Matcher
 *
 * 注意:
 * MVCC 仅在内存实现, 崩溃恢复直接到最新状态
 * 旧有的数据还在硬盘, 但相应索引丢失, 有恢复的可能(作者未实现相关功能)
 *
 * 思路:
 * BitDegradeTree 存储最旧的索引
 * 新的数据存入 _pending, 每当有 snapshot 被释放, 转移部分 _pending 到硬盘
 * 通过合并 Tree 和 iterator 实现 MVCC
 */

#include <deque>
#include <map>

#include "index.h"
#include "iterator.h"
#include "seq_gen.h"

namespace LeviDB {
    class IndexMVCC : private BitDegradeTree {
    private:
        typedef std::map<std::string/* k */, OffsetToData/* v */, SliceComparator> history;
        typedef std::pair<uint64_t/* seq_num */, history> bundle;

        SeqGenerator * _seq_gen;
        RandomAccessFile * _v_file;
        std::deque<bundle> _pending;

    public:
        IndexMVCC(const std::string & fname, SeqGenerator * seq_gen, RandomAccessFile * v_file)
                : BitDegradeTree(fname), _seq_gen(seq_gen), _v_file(v_file) {};

        IndexMVCC(const std::string & fname, OffsetToEmpty empty, SeqGenerator * seq_gen, RandomAccessFile * v_file)
                : BitDegradeTree(fname, empty), _seq_gen(seq_gen), _v_file(v_file) {};

        ~IndexMVCC() noexcept = default;

        DEFAULT_MOVE(IndexMVCC);
        DELETE_COPY(IndexMVCC);

    public:
        // 返回最接近 k 的结果
        std::string find(const Slice & k, uint64_t seq_num = 0) const;

        // 以下方法的调用者必须有读写锁保护且快照只读
        // 无需显式提供 seq_num
        void insert(const Slice & k, OffsetToData v);

        void remove(const Slice & k);

        void sync();

    private:
        // 在 input seq_num 时间点之前的所有 pending history
        std::unique_ptr<Iterator> pendingPart(uint64_t seq_num) const noexcept;

        void applyPending();

    private:
        std::unique_ptr<Matcher> offToMatcher(OffsetToData data) const noexcept override;

        std::unique_ptr<Matcher> sliceToMatcher(const Slice & slice) const noexcept override;
    };
}

#endif //LEVIDB_INDEX_MVCC_H