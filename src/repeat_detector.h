#ifndef LEVIDB_REPEAT_DETECTOR_H
#define LEVIDB_REPEAT_DETECTOR_H

/*
 * 使用 GeneralSuffixTree 进行重复检测
 */

#include "arena.h"
#include "slice.h"
#include "util.h"
#include <climits>

namespace LeviDB {
    struct STNode {
        const STNode * successor;
        const STNode * parent;
        STNode * child;
        STNode * sibling;
        uint16_t chunk_idx;
        uint16_t from;
        uint16_t to;
    };

    class STBuilder {
    private:
        int _compress_len = 0;
        int _compress_idx = -1;
        int _compress_to = -1;

        friend class SuffixTree;

    public:
        static constexpr int compress_cost = 1/* FN */+ 1/* chunk_idx */+ 1/* from */+ 1/* to */;
        enum Message {
            STREAM_ON = -1,
            STREAM_OFF = -2,
            STREAM_PASS = -3,
            STREAM_POP = -4,
        };
        std::vector<int> _data;

        STBuilder() noexcept = default;

        ~STBuilder() noexcept = default;

        void send(int chunk_idx_or_cmd = INT_MIN,
                  int s_idx = INT_MIN,
                  int msg_char = INT_MIN) noexcept;

        void send(int chunk_idx, int s_to, int msg_char, int len) noexcept;

        // 禁止复制
        STBuilder(const STBuilder &) = delete;

        void operator=(const STBuilder &) = delete;
    };

    class SuffixTree {
    protected: // for test
        STNode _root_;
        STNode _dummy_;
        STNode * const _root;
        STNode * const _dummy;
        Arena * const _pool;
        const STNode * _act_node;
        const STNode * _edge_node;
        STBuilder _builder;
        std::vector<Slice> _chunk;
        uint16_t _act_chunk_idx;
        uint16_t _act_direct;
        uint16_t _act_offset;
        uint16_t _counter;
        uint16_t _remainder;

        friend class Compressor;

    public:
        explicit SuffixTree(Arena * arena) noexcept;

        ~SuffixTree() noexcept = default;

        std::vector<int> setitem(const Slice & src) noexcept;

        std::string toString() const noexcept; // debug only

        // 禁止复制
        SuffixTree(const SuffixTree &) = delete;

        void operator=(const SuffixTree &) = delete;

    protected:
        void insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept;

        void tryExplodeRemainder(uint16_t chunk_idx) noexcept;

        void prepareNext() noexcept;

        STNode * newNode(const STNode & node) noexcept;

        const STNode * nodeSetSub(const STNode & sub) noexcept;

        void nodeMove(const STNode & old_node, const STNode & new_node) noexcept;

        const STNode * nodeGetSub(const STNode * node, uint8_t symbol) const noexcept;

        bool nodeIsRoot(const STNode * node) const noexcept;

        bool nodeIsInner(const STNode * node) const noexcept;

        bool nodeIsLeaf(const STNode * node) const noexcept;

        int nodeCompare(const STNode & a, const STNode & b) const noexcept;
    };
}

#endif //LEVIDB_REPEAT_DETECTOR_H