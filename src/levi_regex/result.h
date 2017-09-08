#ifndef LEVIDB_RESULT_H
#define LEVIDB_RESULT_H

/*
 * 记录字符串已匹配的开始和结束
 * 成功或失败
 */

#include <cassert>
#include <string>

namespace LeviDB {
    namespace Regex {
        class Result {
        public:
            int _op = 1;
            int _ed = 0;
            bool _success{};

        public:
            Result() noexcept = default;

            Result(int op, int ed, bool success) noexcept
                    : _op(op), _ed(ed), _success(success) { assert(!isContinue()); }

            Result & asSuccess() noexcept {
                assert(!isContinue());
                _success = true;
                return *this;
            }

            Result & asFail() noexcept {
                assert(!isContinue());
                _success = false;
                return *this;
            }

            void invert() noexcept {
                assert(!isContinue());
                _success = !_success;
            }

            bool isSuccess() const noexcept {
                assert(!isContinue());
                return _success;
            }

            bool isFail() const noexcept {
                assert(!isContinue());
                return !_success;
            }

            bool isContinue() const noexcept { return _op > _ed; }

            std::string toString() const noexcept {
                std::string r;
                if (isContinue()) {
                    return "Continue";
                } else if (isSuccess()) {
                    r = "Success:";
                } else {
                    r = "Fail:";
                }
                return r + std::to_string(_op) + ',' + std::to_string(_ed);
            }
        };
    }
}

#endif // LEVIDB_RESULT_H