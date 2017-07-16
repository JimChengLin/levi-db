#include "../src/repeat_detector.h"
#include <array>
#include <iostream>

void repeat_detector_test() noexcept {
    class Tester : public LeviDB::SuffixTree {
    public:
        Tester(LeviDB::Arena * arena) noexcept : SuffixTree(arena) {}

        bool contains(const std::string & src, int from) const noexcept {
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

    {
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
    }

    {
        LeviDB::Arena arena;
        LeviDB::SuffixTree suffix_tree(&arena);

        std::array<std::string, 3> sources = {"BAAAAAAAAAAAAAAAX", "BGUKNHOKAAAAAAAAAAA", "GUKNHOKAAUYFBJO"};
        std::vector<std::vector<int>> answers = {{66,  65, 257, 0,  1,  15, 88},
                                                 {66,  71, 85,  75, 78, 72, 79, 75, 257, 0, 1, 12},
                                                 {257, 1,  1,   10, 85, 89, 70, 66, 74,  79}};

        for (int i = 0; i < sources.size(); ++i) {
            assert(suffix_tree.setitem(sources[i]) == answers[i]);
        }
    }

    std::cout << __FUNCTION__ << std::endl;
};