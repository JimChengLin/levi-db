#ifndef LEVIDB_INDEX_H
#define LEVIDB_INDEX_H

/*
 * 比特退化树(基类)
 * https://zhuanlan.zhihu.com/p/27071075
 */

#include <array>
#include <cstdint>
#include <cassert>
#include <random>

#ifndef __clang__
#include <memory>
#include <tuple>
#endif

#include "env_io.h"
#include "slice.h"
#include "usr.h"
#include "util.h"

namespace LeviDB {
    namespace IndexConst {
        static constexpr int rank_ = 454;
        // 索引和数据文件最大为 4GB, 又 sizeof(record) > 4, 所以 UINT32_MAX 可以作为 NULL 使用
        static constexpr uint32_t disk_null_ = UINT32_MAX; // 无效且不合法
    }

    inline bool isSpecialMask(uint8_t mask) noexcept {
        return mask > 0 && ((((mask >> 2) & 1) + ((mask >> 1) & 1) + (mask & 1)) < 2);
    }

    inline uint8_t normalMask(uint8_t mask) noexcept {
        bool is_spec = isSpecialMask(mask);
        uint8_t m = 0;
        m |= static_cast<uint8_t>(is_spec);
        m |= (m << 1);
        m |= (m << 2);
        m |= (m << 4);
        return mask ^ m;
    }

    // 数据文件中单条 record 的偏移量
    struct OffsetToData {
        uint32_t val;
    };

    // 索引文件中 node 的偏移量
    struct OffsetToNode {
        uint32_t val;
    };

    // 索引文件中 empty struct 的偏移量
    struct OffsetToEmpty {
        uint32_t val;
    };

    class CritPtr {
    private:
        // 默认所有 input offset 都是 2bytes 对齐
        // 奇数表示 OffsetToNode
        // 偶数表示 OffsetToData
        uint32_t _offset = IndexConst::disk_null_;

    public:
        bool isNull() const noexcept {
            return _offset == IndexConst::disk_null_;
        }

        bool isData() const noexcept {
            assert(!isNull());
            return (_offset & 1) == 0;
        }

        bool isNode() const noexcept {
            return !isData();
        }

        void setNull() noexcept {
            _offset = IndexConst::disk_null_;
        }

        void setData(uint32_t offset) noexcept {
            _offset = offset;
            assert(isData());
        }

        void setData(OffsetToData data) noexcept {
            setData(data.val);
            assert(isData());
        }

        void setNode(uint32_t offset) noexcept {
            _offset = offset + 1;
            assert(isNode());
        }

        void setNode(OffsetToNode node) noexcept {
            setNode(node.val);
            assert(isNode());
        }

        OffsetToData asData() const noexcept {
            assert(isData());
            return {_offset};
        }

        OffsetToNode asNode() const noexcept {
            assert(isNode());
            return {_offset - 1};
        }
    };

    // 释放的节点写为 BDEmpty 对象以链表的方式串联, 用于 reallocate
    class BDEmpty {
    private:
        OffsetToEmpty _next{IndexConst::disk_null_};
        std::array<uint8_t, 4> _checksum{};

    public:
        bool verify() const noexcept;

        void updateChecksum() noexcept;

        EXPOSE(_next);
    };

    class BDNode {
    private:
        std::array<CritPtr, IndexConst::rank_ + 1> _ptrs{};
        std::array<uint32_t, IndexConst::rank_> _diffs{};
        std::array<uint8_t, IndexConst::rank_> _masks{};

        std::array<uint8_t, 4> _checksum{};
        std::array<uint8_t, 2> _padding_{};

    public:
        bool full() const noexcept { return !_ptrs.back().isNull(); };

        size_t size() const noexcept;

        auto functor() const noexcept {
            return [&](const uint32_t & a, const uint32_t & b) noexcept {
                if (a < b) {
                    return true;
                }
                if (a == b) {
                    return normalMask(_masks[&a - _diffs.cbegin()]) < normalMask(_masks[&b - _diffs.cbegin()]);
                }
                return false;
            };
        };

        bool verify() const noexcept;

        void updateChecksum() noexcept;

        EXPOSE(_ptrs);

        EXPOSE(_diffs);

        EXPOSE(_masks);
    };

    static_assert(sizeof(BDNode) == 4096, "align for mmap");
    static_assert(std::is_standard_layout<BDNode>::value, "align for mmap");

    // 用户提供的 k 和索引指向的已存 kv 的偏移量, 二者的语义大体相同
    // 但实现不同, 故用接口统一
    class Matcher {
    public:
        Matcher() noexcept = default;
        DEFAULT_MOVE(Matcher);
        DEFAULT_COPY(Matcher);

    public:
        virtual ~Matcher() noexcept = default;

        virtual char operator[](size_t idx) const = 0;

        virtual bool operator==(const Matcher & another) const = 0;

        virtual bool operator==(const Slice & another) const = 0;

        virtual size_t size() const = 0;

        virtual std::string toString() const = 0;

        // Matcher 有可能包装 multi-KV batch, target 用于区分
        virtual std::string toString(const Slice & target) const { return toString(); };

        virtual std::string getValue(const Slice & target) const { return {}; };
    };

    class BitDegradeTree {
    protected:
        MmapFile _dst;
        OffsetToNode _root{0};
        OffsetToEmpty _empty{IndexConst::disk_null_};
        std::default_random_engine _gen{std::random_device{}()};

    public:
        explicit BitDegradeTree(const std::string & fname)
                : _dst(fname) {
            assert(_dst.immut_length() == IOEnv::page_size_);
            (new(offToMemNodeUnchecked(_root)) BDNode)->updateChecksum();
        };

        BitDegradeTree(const std::string & fname, OffsetToEmpty empty) : _dst(fname), _empty(empty) {};

        ~BitDegradeTree() noexcept = default;

        DEFAULT_MOVE(BitDegradeTree);
        DELETE_COPY(BitDegradeTree);

    public:
        // 返回最接近 k 的结果, 调用者需要二次检查是否真正匹配
        // null_disk_ 说明未找到
        OffsetToData find(const Slice & k) const;

        // 当有 compress record 时, size 不准确, 仅用作内部测试
        size_t size() const { return size(offToMemNode(_root)); };

        size_t size(const BDNode * node) const;

        void insert(const Slice & k, OffsetToData v);

        void remove(const Slice & k, OffsetToData v);

        void sync() { _dst.sync(); };

    private:
        std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
        findBestMatch(const BDNode * node, const Slice & k) const noexcept;

        std::tuple<size_t/* idx */, bool/* direct */, size_t/* size */>
        findBestMatch(const BDNode * node, const Slice & k, USR * reveal_info) const noexcept;

        void combatInsert(const Slice & opponent, const Slice & k, OffsetToData v);

        void nodeInsert(BDNode * node, size_t replace_idx, bool replace_direct,
                        bool direct, uint32_t diff_at, uint8_t mask, OffsetToData v,
                        size_t size) noexcept;

        void makeRoom(BDNode * parent);

        void makeRoomPush(BDNode * parent, BDNode * child, size_t idx, bool direct) noexcept;

        void makeNewRoom(BDNode * parent);

        void nodeRemove(BDNode * node, size_t idx, bool direct, size_t size) noexcept;

        void tryMerge(BDNode * parent, BDNode * child,
                      size_t idx, bool direct, size_t parent_size,
                      size_t child_size) noexcept;

        USR mostSimilarUsr(const Slice & k) const;

    protected:
        BDNode * offToMemNode(OffsetToNode node) const;

        BDNode * offToMemNodeUnchecked(OffsetToNode node) const noexcept;

        BDEmpty * offToMemEmpty(OffsetToEmpty empty) const;

        // 继承时覆盖
        virtual std::unique_ptr<Matcher> offToMatcher(OffsetToData data) const noexcept;

        virtual std::unique_ptr<Matcher> sliceToMatcher(const Slice & slice) const noexcept;

        OffsetToNode mallocNode();

        void freeNode(OffsetToNode node) noexcept;
    };
}

#endif //LEVIDB_INDEX_H