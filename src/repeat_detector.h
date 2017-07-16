#ifndef LEVIDB_REPEAT_DETECTOR_H
#define LEVIDB_REPEAT_DETECTOR_H

/*
 * 使用 GeneralSuffixTree 进行重复检测
 */

#include "arena.h"
#include "skiplist.h"
#include "slice.h"
#include "util.h"
#include <climits>

namespace LeviDB {
    struct STNode {
        const STNode * successor;
        const STNode * parent;
        uint16_t chunk_idx;
        uint16_t from;
        uint16_t to;
    };

    class STBuilder {
    private:
        int _compress_len;
        int _compress_idx;
        int _compress_to;

    public:
        enum Message {
            STREAM_ON = -1,
            STREAM_OFF = -2,
            STREAM_PASS = -3,
            STREAM_POP = -4,
        };
        std::vector<int> _data;

        STBuilder() noexcept : _data() {}

        ~STBuilder() noexcept {}

        void send(int chunk_idx_or_cmd = INT_MIN,
                  int s_idx = INT_MIN,
                  int msg_char = INT_MIN) noexcept;

    private:
        // 禁止复制
        STBuilder(const STBuilder &);

        void operator=(const STBuilder &);
    };

    class SuffixTree {
    protected: // for test
        struct NodeCompare {
            const std::vector<Slice> & chunk;

            int operator()(const STNode & a, const STNode & b) const noexcept {
                if (a.parent < b.parent) {
                    return -1;
                } else if (a.parent == b.parent) {
                    uint8_t a_val = a.from > a.to ?
                                    static_cast<uint8_t>(a.chunk_idx) : char_be_uint8(chunk[a.chunk_idx][a.from]);
                    uint8_t b_val = b.from > b.to ?
                                    static_cast<uint8_t>(b.chunk_idx) : char_be_uint8(chunk[b.chunk_idx][b.from]);
                    if (a_val < b_val) {
                        return -1;
                    } else if (a_val == b_val) {
                        return 0;
                    } else {
                        return 1;
                    }
                } else {
                    return 1;
                }
            }
        };

        Arena * const _pool;
        STNode * const _root;
        const STNode * _act_node;
        const STNode * _edge_node;
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

        std::string toString() const noexcept; // debug only

    private:
        void insertChar(uint16_t chunk_idx, uint8_t msg_char) noexcept;

        // 禁止复制
        SuffixTree(const SuffixTree &);

        void operator=(const SuffixTree &);

    protected:
        STNode * newNode() noexcept;

        const STNode * nodeSetSub(const STNode & sub) noexcept;

        const STNode * nodeGetSub(const STNode * node, uint8_t key) const noexcept;

        bool nodeIsRoot(const STNode * node) const noexcept;

        bool nodeIsInner(const STNode * node) const noexcept;

        bool nodeIsLeaf(const STNode * node) const noexcept;
    };
}

#endif //LEVIDB_REPEAT_DETECTOR_H