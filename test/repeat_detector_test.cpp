#include "../src/repeat_detector.h"
#include <array>
#include <iostream>

void repeat_detector_test() {
    class Tester : public LeviDB::SuffixTree {
    public:
        Tester(LeviDB::Arena * arena) noexcept : SuffixTree(arena) {}

        bool contains(std::string & src, int from) const noexcept {
            const LeviDB::STNode * edge_node = nodeGetSub(_root, LeviDB::char_be_uint8(src[from]));
            if (edge_node == nullptr) {
                return false;
            }

            while (true) {
                const LeviDB::Slice edge_s = _chunk[edge_node->chunk_idx];
                for (int i = edge_node->from; i < edge_node->to; ++i) {
                    if (edge_s.data()[i] != src[from]) {
                        return false;
                    } else {
                        ++from;
                        if (from == src.size()) {
                            return true;
                        }
                    }
                }
                edge_node = nodeGetSub(edge_node, LeviDB::char_be_uint8(src[from]));
            }
        }
    };

    LeviDB::Arena arena;
    Tester tester(&arena);

    srand(19950207);
    static constexpr int n = 100;
    std::array<std::string, n> sources;
    std::array<char, 5> alphabet = {'A', 'B', 'C', 'D', 'E'};

    for (int i = 0; i < n; ++i) {
        std::string sample;
        int len = rand() % 20 + 1;
        for (int j = 0; j < len; ++j) {
            sample += alphabet[rand() % alphabet.size()];
        }
        sample += '.';
        sources[i] = std::move(sample);

        tester.setitem(LeviDB::Slice(sources[i]));
        for (int j = 0; j < sample.size(); ++j) {
            assert(tester.contains(sample, j));
        }
    }

    std::cout << __FUNCTION__ << std::endl;
};