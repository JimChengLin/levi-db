#ifndef LEVIDB_REPEAT_DETECTOR_H
#define LEVIDB_REPEAT_DETECTOR_H

/*
 * 使用 GeneralSuffixTree 进行重复检测
 */

#include "arena.h"
#include "skiplist.h"
#include "slice.h"

namespace LeviDB {
    struct STNode {
        STNode * successor;
        STNode * parent;
        uint16_t chunk_idx;
        uint16_t from;
        uint16_t to;
    };

    class STBuilder {
    private:

    public:
        enum Message {
            STREAM_ON = -1,
            STREAM_OFF = -2,
            STREAM_PASS = -3,
        };

        std::vector<int> res;

        STBuilder() noexcept : res() {}

        void send(int chunk_idx_or_cmd = INT_MIN,
                  int s_idx = INT_MIN,
                  int msg_char = INT_MIN) noexcept;
    };

    class SuffixTree {
    private:
        struct NodeCompare {
            const std::vector<Slice> & chunk;

            int operator()(const STNode & a, const STNode & b) const noexcept {
                if (a.parent < b.parent) {
                    return -1;
                } else if (a.parent == b.parent) {
                    char a_char = a.from > a.to ? static_cast<char>(a.chunk_idx) : chunk[a.chunk_idx][a.from];
                    char b_char = b.from > b.to ? static_cast<char>(b.chunk_idx) : chunk[b.chunk_idx][b.from];
                    if (a_char < b_char) {
                        return -1;
                    } else if (a_char == b_char) {
                        return 0;
                    } else {
                        return 1;
                    }
                } else {
                    return 1;
                }
            }
        };

        STNode * _root;
        STNode * _act_node;
        Arena * _pool;
        SkipList<STNode, NodeCompare> _subs;
        STBuilder _builder;
        std::vector<Slice> _chunk;
        uint16_t _act_chunk_idx;
        uint16_t _act_direct;
        uint16_t _act_offset;
        uint16_t _counter;
        uint16_t _remainder;

    public:
        explicit SuffixTree(Arena * arena) noexcept;

        ~SuffixTree() noexcept {};

        std::vector<int> setitem(const Slice & src) noexcept;

        void prepareNext() noexcept;

    private:
        void insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept;

        // 禁止复制
        SuffixTree(const SuffixTree &);

        void operator=(const SuffixTree &);

    private:
        STNode * newNode() noexcept;

        STNode * nodeGetSub(STNode * node, uint8_t key) const noexcept;

        void nodeSetSub(const STNode & sub) noexcept;

        bool nodeIsRoot(STNode * node) const noexcept;

        bool nodeIsInner(STNode * node) const noexcept;

        bool nodeIsLeaf(STNode * node) const noexcept;
    };
}

#endif //LEVIDB_REPEAT_DETECTOR_H