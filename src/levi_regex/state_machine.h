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
        private:
            std::string _pattern;
            Result _result{0, 0, false};
            Result _output;
            int _line = 0;
            int i = {};
            int j = {};
            Input _input{};

        public:
            explicit StateMachine(std::string pattern) noexcept
                    : _pattern(std::move(pattern)) { next(); }

            DEFAULT_MOVE(StateMachine);

            StateMachine(const StateMachine &) = default;

            StateMachine & operator=(const StateMachine &) = default;

            ~StateMachine() noexcept = default;

        public:
            bool valid() const noexcept { return _line != -1; };

            void setResult(Result result) noexcept { _result = result; }

            Result send(Input input) noexcept;

        private:
            void next() noexcept;
        };
    }
}

#endif // LEVIDB_STATE_MACHINE_H