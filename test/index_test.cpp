#include "../src/index.h"
#include <iostream>

void index_test() {
    {
        char * a = new char[2];
        char * b = new char[2];
        char * c = new char[2];
        a[0] = 'A';
        a[1] = 0;
        b[0] = 'B';
        b[1] = 0;
        c[0] = 'C';
        c[1] = 0;

        LeviDB::BitDegradeTree tree;
        tree.insert(a);
        tree.insert(c);
        tree.insert(b);

        assert(tree._root->_masks[0] == 0b11111101);
        assert(tree._root->_masks[1] == 0b11111110);
        assert(tree._root->_ptrs[0].asVal()[0] == 'A');
        assert(tree._root->_ptrs[1].asVal()[0] == 'B');
        assert(tree._root->_ptrs[2].asVal()[0] == 'C');

        char * x = new char[2];
        x[0] = 0b1000000;
        x[1] = 0;
        tree.insert(x);

        x = new char[2];
        x[0] = 0b1010000;
        x[1] = 0;
        tree.insert(x);

        assert(tree._root->_ptrs[0].isNode());
        assert(tree._root->_masks[2] == 0b11101111);

        x = new char[2];
        x[0] = 0b0000001;
        x[1] = 0;
        tree.insert(x);
        assert(tree._root->_masks[0] == 0b10111111);

        for (const char * ptr:{"P", "B", "C"}) {
            tree.remove(ptr);
        }
        assert(tree._root->_ptrs[2].isNull() && tree._root->_ptrs[3].isNull());
    }

    {
        char * a = new char[2];
        char * b = new char[2];
        char * c = new char[2];
        a[0] = 'A';
        a[1] = 0;
        b[0] = 'F';
        b[1] = 0;
        c[0] = 'G';
        c[1] = 0;

        LeviDB::BitDegradeTree tree;
        tree.insert(a);
        tree.insert(c);
        tree.insert(b);

        char * x = new char[2];
        x[0] = 'H';
        x[1] = 0;
        tree.insert(x);

        x = new char[2];
        x[0] = 'I';
        x[1] = 0;
        tree.insert(x);

        x = new char[2];
        x[0] = 'B';
        x[1] = 0;
        tree.insert(x);

        for (const char * ptr:{"A", "G", "F", "B", "I", "H"}) {
            assert(tree.find(ptr)[0] == ptr[0]);
        }
        assert(tree.find("_")[0] != '_');

        tree.remove("A");
        assert(tree._root->size() == 4 && tree._root->_ptrs[0].asNode()->size() == 2);

        for (const char * ptr:{"G", "F", "B", "I", "H"}) {
            assert(tree.find(ptr)[0] == ptr[0]);
        }
    }

    std::cout << __FUNCTION__ << std::endl;
}