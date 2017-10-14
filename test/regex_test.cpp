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
        auto clone = LeviDB::USR("ABABABAB");
        LeviDB::USR usr(std::move(clone));
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
        LeviDB::Regex::cacheClear();
    }
    {
        typedef LeviDB::Regex::R R;
        LeviDB::Regex::Result result(0, 0, false);
        LeviDB::USR usr("ABABABAB");

        R r = R("AB") & R("AB");
        LeviDB::Regex::Result output;
        auto it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isSuccess() && output._ed == 2);
        LeviDB::Regex::cacheClear();

        r = (R("AB") << R("UNKNOWN", 0, 0)) & (R("AB") << R(std::make_unique<R>("UNKNOWN"), 0, 0));
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isSuccess() && output._ed == 2);
        LeviDB::Regex::cacheClear();

        r = R("A") << R(std::make_unique<R>("B"), 0, 1, LeviDB::Regex::GREEDY);
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            if (!output.isContinue() && output.isSuccess()) {
                assert(output._ed == 2);
                break;
            }
            it->next();
        }
        LeviDB::Regex::cacheClear();

        r = R("A") << R(std::make_unique<R>("B"), 0, 1, LeviDB::Regex::LAZY);
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            if (!output.isContinue() && output.isSuccess()) {
                assert(output._ed == 1);
                break;
            }
            it->next();
        }
        LeviDB::Regex::cacheClear();

        r = R(std::make_unique<R>("AB"), 1, 3);
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            if (!output.isContinue() && output.isSuccess()) {
                assert(output._ed == 6);
                break;
            }
            it->next();
        }
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output._ed == 2);
        LeviDB::Regex::cacheClear();

        r = R("AB") & R("AC");
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isFail() && output._ed == 2);
        LeviDB::Regex::cacheClear();

        r = R("AC") | R("AB");
        int meet_success = 0;
        int meet_fail = 0;
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            if (it->item().isContinue()) {
            } else if (it->item().isSuccess()) {
                ++meet_success;
            } else {
                ++meet_fail;
            }
            it->next();
        }
        assert(meet_success == 1 && meet_fail == 1);
        LeviDB::Regex::cacheClear();

        r = ~R("B");
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isSuccess() && output._ed == 1);
        LeviDB::Regex::cacheClear();

        r = R("A") << "B";
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isSuccess() && output._ed == 2);
        LeviDB::Regex::cacheClear();

        r = (R("AB", 0, 3) | R("ABABAB")) << R("C");
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isFail());
        LeviDB::Regex::cacheClear();

        usr = LeviDB::USR("FGH");
        r = R("A", "Z");
        it = LeviDB::Regex::make_imatch_iter(&r, &usr, result);
        while (it->valid()) {
            output = it->item();
            it->next();
        }
        assert(output.isSuccess() && output._ed == 1);
        LeviDB::Regex::cacheClear();
    }
    {
        LeviDB::Regex::enablePossibleMode();
        LeviDB::Regex::R r("B");
        LeviDB::Regex::Result result(0, 0, false);
        LeviDB::USR usr;
        usr.mut_src()->resize(1);
        usr.mut_extra().resize(1);
        auto stream4num_machine = LeviDB::Regex::make_stream4num_machine(&r, &usr, result);
        stream4num_machine->next();
        assert(LeviDB::Regex::getPossibleResultRef().isSuccess());
        LeviDB::Regex::getPossibleResultRef() = {};
        LeviDB::Regex::disablePossibleMode();
    }

    std::cout << __FUNCTION__ << std::endl;
}