#ifndef LEVIDB_INDEX_REGEX_ITER_H
#define LEVIDB_INDEX_REGEX_ITER_H

/*
 * 在已有基础上, 实现 iter 和 regex 的比特退化树
 *
 * iter 思路:
 * 在 valid 期间, index 不允许 applyPending, 那会破坏递归结构
 * 与普通 CritBit Tree 一样 iterate, 但最后要和 pending 做归并以符合 MVCC 要求
 *
 * regex 思路:
 * 构造一个 regex 解释器, 返回状态: OK, NO, POSSIBLE
 * 在 trie 分叉的地方检测, 如果 OK || POSSIBLE 说明可以继续下去
 *
 * 解释器的输入为"universal string representation"(USR), 按 bit 描述数据, 类型: 1 || 0 || UNKNOWN
 * 比如, 匹配"A"的解释器应该对
 * USR"1 0 0 0 0 0 UNKNOWN" => OK & POSSIBLE
 *    "1 1 1 0 0 0 0"       => NO
 *    "1 0 0"               => POSSIBLE
 *
 * 使用 URS 的原因是 CritBitTree 每次分歧只 reveal 1bit 的信息
 * return 时必须 OK(全匹配)
 */

#include "index_mvcc_rd.h"
#include "levi_regex/r.h"

namespace LeviDB {
    class IndexIter : public IndexRead {
    private:
        template<bool RIGHT_FIRST>
        class BitDegradeTreeNodeIter;

        typedef BitDegradeTreeNodeIter<false> ForwardNodeIter;
        typedef BitDegradeTreeNodeIter<true> BackwardNodeIter;

        class BitDegradeTreeIterator;

        class TreeIteratorMerged;

        class TreeIteratorFiltered;

        mutable std::atomic<int> _operating_iters{0};

        friend class IndexRegex;

    public:
        using IndexRead::IndexRead;

        ~IndexIter() noexcept { assert(_operating_iters == 0); };

        DELETE_MOVE(IndexIter);
        DELETE_COPY(IndexIter);

    public:
        std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator(std::unique_ptr<Snapshot> && snapshot) const noexcept;

        std::unique_ptr<Iterator<Slice, std::string>>
        makeIterator() const noexcept { return makeIterator(_seq_gen->makeSnapshot()); };

        void tryApplyPending();
    };

    class IndexRegex : public IndexIter {
    private:
        class RegexIterator;

        class ReversedRegexIterator;

    public:
        using IndexIter::IndexIter;

        ~IndexRegex() noexcept = default;

        DELETE_MOVE(IndexRegex);
        DELETE_COPY(IndexRegex);

    public:
        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex, std::unique_ptr<Snapshot> && snapshot) const;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexIterator(std::shared_ptr<Regex::R> regex) const {
            return makeRegexIterator(std::move(regex), _seq_gen->makeSnapshot());
        };

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const;

        std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
        makeRegexReversedIterator(std::shared_ptr<Regex::R> regex) const {
            return makeRegexReversedIterator(std::move(regex), _seq_gen->makeSnapshot());
        };
    };
}

#endif //LEVIDB_INDEX_REGEX_ITER_H