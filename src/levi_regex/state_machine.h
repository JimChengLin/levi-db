#ifndef LEVIDB_STATE_MACHINE_H
#define LEVIDB_STATE_MACHINE_H

/*
 * 正则解析用状态机
 */

#include "../util.h"
#include "result.h"

namespace LeviDB {
    namespace Regex {
        enum Input {
            FALSE = 0,
            TRUE = 1,
            UNKNOWN = 2,
        };

        class StateMachine {
        protected:
            std::string _pattern;
            Result _result{0, 0, false};
            Result _output;
            int _line = 0;
            int i{};
            int j{};
            Input _input{};

        public:
            explicit StateMachine(std::string pattern) noexcept
                    : _pattern(std::move(pattern)) { next(); }

            DEFAULT_MOVE(StateMachine);

            StateMachine(const StateMachine &) = default;

            StateMachine & operator=(const StateMachine &) = default;

            virtual ~StateMachine() noexcept = default;

        public:
            bool valid() const noexcept { return _line != -1; }

            void setResult(Result result) noexcept { _result = result; }

            Result send(Input input) noexcept;

            virtual void reset() noexcept;

        protected:
            StateMachine() = default;

            virtual void next() noexcept;
        };

        class RangeStateMachine : public StateMachine {
        private:
            std::string _pattern_to;

            std::string _lower_bound;
            std::string _upper_bound;

        public:
            RangeStateMachine(std::string pattern, std::string pattern_to) noexcept
                    : _pattern_to(std::move(pattern_to)),
                      _lower_bound(pattern.size(), 0), _upper_bound(pattern.size(), uint8ToChar(UINT8_MAX)) {
                _pattern = std::move(pattern);
                assert(_pattern.size() == _pattern_to.size() && _pattern <= _pattern_to);
                next();
            }

            DEFAULT_MOVE(RangeStateMachine);

            RangeStateMachine(const RangeStateMachine &) = default;

            RangeStateMachine & operator=(const RangeStateMachine &) = default;

            ~RangeStateMachine() noexcept override = default;

            void reset() noexcept override;

        private:
            void next() noexcept override;
        };
    }
}

#endif // LEVIDB_STATE_MACHINE_H