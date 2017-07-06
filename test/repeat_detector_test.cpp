#include "../src/repeat_detector.h"

void repeat_detector_test() {
    using namespace LeviDB;

    class Tester : public SuffixTree {
    private:
        Arena arena;

    public:
        Tester() noexcept
                : arena(), SuffixTree(&arena) {}

        bool contains(std::string & src) {
            const STNode * edge_node = nodeGetSub(_root, char_be_uint8(src[0]));

        }
    };

};