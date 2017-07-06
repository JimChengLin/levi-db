#include "../src/repeat_detector.h"

void repeat_detector_test() {
    using namespace LeviDB;

    class Tester : public SuffixTree {
    private:
        Arena arena;

    public:
        Tester() noexcept : arena(), SuffixTree(&arena) {}

        bool contains(std::string & src, int from) {
            const STNode * edge_node = nodeGetSub(_root, char_be_uint8(src[from]));
            if (edge_node == nullptr) {
                return false;
            }

            while (true) {
                const Slice edge_s = _chunk[edge_node->chunk_idx];
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
                edge_node = nodeGetSub(edge_node, char_be_uint8(src[from]));
            }
        }
    };

    Tester tester;
    srand(19950207);
    char alphabet[] = {'A', 'B', 'C', 'D', 'E'};
    for (int i = 0; i < 100; ++i) {
        std::string sample;
        int len = rand() % 20 + 1;
        for (int j = 0; j < len; ++j) {
            sample += alphabet[rand() % 5];
        }
        sample += '.';

        tester.setitem(sample);
        for (int j = 0; j < sample.size(); ++j) {
            assert(tester.contains(sample, j));
        }
    }
};