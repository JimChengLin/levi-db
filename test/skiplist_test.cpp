#include "../src/skiplist.h"
#include <iostream>

void skiplist_test() {
    char src[] = {'D', 'E', 'I', 'K', 'M', 'N', 'O', 'Q', 'W', 'X', 'Z'};

    LeviDB::Arena arena;
    LeviDB::SkipList<char> skip_list(&arena);
    for (char c:src) {
        skip_list.insert(c);
    }

    LeviDB::SkipList<char>::Iterator it(&skip_list);
    it.seekToFirst();
    while (it.valid()) {
        std::cout << it.key() << std::endl;
        it.next();
    }
}