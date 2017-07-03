#include "../src/skiplist.h"
#include <iostream>

void skiplist_test() {
    char src[] = {'X', 'Z', 'N', 'O', 'Q', 'D', 'E', 'I', 'K', 'M', 'W'};
    char ans[] = {'D', 'E', 'I', 'K', 'M', 'N', 'O', 'Q', 'W', 'X', 'Z'};

    LeviDB::Arena arena;
    LeviDB::SkipList<char> skip_list(&arena);
    for (char c:src) {
        skip_list.insert(c);
    }

    LeviDB::SkipList<char>::Iterator it(&skip_list);
    it.seekToFirst();
    int i = 0;
    while (it.valid()) {
        assert(it.key() == ans[i++]);
        it.next();
    }

    std::cout << __FUNCTION__ << std::endl;
}