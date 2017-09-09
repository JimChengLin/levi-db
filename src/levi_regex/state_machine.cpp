#include "state_machine.h"

namespace LeviDB {
    namespace Regex {
        void StateMachine::reset() noexcept {
            _result = {};
            _output = {};
            _line = 0;
            i = {};
            j = {};
            _input = {};
            next();
        }

        Result StateMachine::send(Input input) noexcept {
            _input = input;
            next();
            return _output;
        }

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;

        void StateMachine::next() noexcept {
            assert(valid());
            GEN_INIT();
                for (i = 0; i < _pattern.size(); ++i) {
                    for (j = 7; j >= 0; --j) {
                        YIELD();
                        if (_input == UNKNOWN || ((((_pattern[i] >> j) ^ _input) & 1) == 0)) {
                        } else {
                            _output = _result.asFail();
                            YIELD();
                        }
                    }
                    ++_result._ed;
                }
                _output = _result.asSuccess();
                YIELD();
            GEN_STOP();
        };
    }
}