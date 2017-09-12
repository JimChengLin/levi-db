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

        void RangeStateMachine::reset() noexcept {
            StateMachine::reset();
            std::fill(_lower_bound.begin(), _lower_bound.end(), 0);
            std::fill(_upper_bound.begin(), _upper_bound.end(), uint8ToChar(UINT8_MAX));
        }

        Result StateMachine::send(Input input) noexcept {
            _input = input;
            next();
            return _output;
        }

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;
#define RETURN() _line = -1; return;

        void StateMachine::next() noexcept {
            assert(valid());
            GEN_INIT();
                for (i = 0; i < _pattern.size(); ++i) {
                    for (j = 7; j >= 0; --j) {
                        YIELD();
                        if (_input == UNKNOWN || ((((_pattern[i] >> j) ^ _input) & 1) == 0)) {
                        } else {
                            ++_result._ed;
                            _output = _result.asFail();
                            YIELD();
                            RETURN();
                        }
                    }
                    ++_result._ed;
                }
                _output = _result.asSuccess();
                YIELD();
            GEN_STOP();
        }

        void RangeStateMachine::next() noexcept {
            assert(valid());
            GEN_INIT();
                for (i = 0; i < _pattern.size(); ++i) {
                    for (j = 7; j >= 0; --j) {
                        YIELD();
                        switch (_input) {
                            case TRUE: // lower bound become upper, 0 => 1
                                assert(((_lower_bound[i] >> j) & 1) == 0);
                                _lower_bound[i] |= (1 << j);
                                break;

                            case FALSE: // upper bound become lower, 1 => 0
                                assert(((_upper_bound[i] >> j) & 1) == 1);
                                _upper_bound[i] ^= (1 << j);
                                break;

                            case UNKNOWN:
                                break;
                        }
                    }
                    ++_result._ed;
                }
                _output = std::min(_pattern_to, _upper_bound) >= std::max(_pattern, _lower_bound) ? _result.asSuccess()
                                                                                                  : _result.asFail();
                YIELD();
            GEN_STOP();
        }
    }
}