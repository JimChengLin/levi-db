#include <iostream>

#include "../src/levi_regex/r.h"
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
    {
        LeviDB::Regex::R r("AB", 2, 3, LeviDB::Regex::LAZY);
        LeviDB::Regex::Result result(0, 0, false);
        LeviDB::USR usr("ABABABAB");
        auto stream4num_machine = LeviDB::Regex::make_stream4num_machine(&r, &usr, result);
        assert(stream4num_machine->item().isContinue());
        stream4num_machine->next();
        assert(stream4num_machine->item().isSuccess() && stream4num_machine->item()._ed == 4);
        stream4num_machine->next();
        assert(stream4num_machine->item().isSuccess() && stream4num_machine->item()._ed == 6);
        stream4num_machine->next();
        assert(!stream4num_machine->valid());

        auto reversed = LeviDB::Regex::make_reversed(LeviDB::Regex::make_stream4num_machine(&r, &usr, result));
        assert(reversed->item().isSuccess() && reversed->item()._ed == 6);
        reversed->next();
        assert(reversed->item().isSuccess() && reversed->item()._ed == 4);
        reversed->next();
        assert(reversed->item().isContinue());
        reversed->next();
        assert(!reversed->valid());
    }
    {
        LeviDB::Regex::R r(std::make_unique<LeviDB::Regex::R>("AB"), 2, 3, LeviDB::Regex::LAZY);
        LeviDB::Regex::Result result(0, 0, false);
        LeviDB::USR usr("ABABABAB");
        auto stream4num_r = LeviDB::Regex::make_stream4num_r(&r, &usr, result);
        assert(stream4num_r->item().isContinue());
        stream4num_r->next();
        assert(stream4num_r->item().isSuccess() && stream4num_r->item()._ed == 4);
        stream4num_r->next();
        assert(stream4num_r->item().isSuccess() && stream4num_r->item()._ed == 6);
        stream4num_r->next();
        assert(!stream4num_r->valid());
    }

    std::cout << __FUNCTION__ << std::endl;
}