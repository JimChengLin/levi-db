#include <iostream>

#include "../src/levi_regex/state_machine.h"

void regex_test() {
    {
        LeviDB::Regex::Result result;
        LeviDB::Regex::StateMachine machine("Hello");
        for (char c:{'H', 'e', 'l', 'l', 'o'}) {
            for (int i = 7; i >= 0; --i) {
                result = machine.send(static_cast<LeviDB::Regex::Input>((c >> i) & 1));
            }
        }
        assert(result.isSuccess());
    }

    std::cout << __FUNCTION__ << std::endl;
}