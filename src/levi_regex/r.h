#ifndef LEVIDB_R_H
#define LEVIDB_R_H

/*
 * 为 BitDegradeTree 定制的 NFA 正则查询引擎
 * API 和大体设计来自于作者的项目 EasyRegex:
 * https://github.com/JimChengLin/EasyRegex
 */

#include <memory>

#include "../iterator.h"
#include "../usr.h"
#include "result.h"

namespace LeviDB {
    namespace Regex {
        enum Mode {
            GREEDY,
            LAZY,
        };

        class R : public UsrJudge {
        private:
            enum Relation {
                AND,
                OR,
                XOR, // replace with A ^ B = (A | B) & ~(A & B)
                INVERT,
                NEXT,
                NONE,
            };

            // _r 和 _pattern 二者不可能同时有效
            std::unique_ptr<R> _r;
            std::string _pattern;
            std::string _pattern_to;

            int _num_from = 1;
            int _num_to = 1;

            std::unique_ptr<R> _other;
            Relation _relation = NONE;
            Mode _mode = GREEDY;

            friend class stream4num_machine;

            friend class stream4num_r;

            friend class and_r_iter;

            friend class or_r_iter;

            friend class next_r_iter;

            friend class imatch_iter;

        public:
            explicit R(std::unique_ptr<R> && r) noexcept : _r(std::move(r)) {}

            explicit R(std::string pattern) noexcept : _pattern(std::move(pattern)) {}

            R(std::string pattern, std::string pattern_to) noexcept
                    : _pattern(std::move(pattern)), _pattern_to(std::move(pattern_to)) {}

            R(std::unique_ptr<R> && r, int num_from, int num_to, Mode mode) noexcept
                    : _r(std::move(r)), _num_from(num_from), _num_to(num_to), _mode(mode) {}

            R(std::string pattern, int num_from, int num_to, Mode mode) noexcept
                    : _pattern(std::move(pattern)), _num_from(num_from), _num_to(num_to), _mode(mode) {}

            R(std::string pattern, std::string pattern_to, int num_from, int num_to, Mode mode) noexcept
                    : _pattern(std::move(pattern)), _pattern_to(std::move(pattern_to)),
                      _num_from(num_from), _num_to(num_to), _mode(mode) {}

            DEFAULT_MOVE(R);

            R(const R & another) noexcept { operator=(another); };

            R & operator=(const R & another) noexcept;

        public:
            R operator&(R another) const & noexcept;

            R operator&(R another) && noexcept;

            R operator|(R another) const & noexcept;

            R operator|(R another) && noexcept;

            R operator^(R another) const & noexcept;

            R operator^(R another) && noexcept;

            R operator~() const & noexcept;

            R operator~() && noexcept;

            R operator<<(R another) const & noexcept;

            R operator<<(R another) && noexcept;

            R operator<<(std::string another) const & noexcept;

            R operator<<(std::string another) && noexcept;

        public:
            ~R() noexcept override = default;

            bool possible(const USR & input) const override;

            bool match(const USR & input) const override;

        private:
            std::unique_ptr<SimpleIterator<Result>>
            imatch(const USR & input, Result prev_result) const noexcept;

            std::unique_ptr<SimpleIterator<Result>>
            imatch(const USR & input, Result prev_result, int from, int to) const noexcept;
        };

        // functions below are just for test
        std::unique_ptr<SimpleIterator<Result>>
        make_stream4num_machine(const R * caller, const USR * src, Result prev_result) noexcept;

        std::unique_ptr<SimpleIterator<Result>>
        make_reversed(std::unique_ptr<SimpleIterator<Result>> && result_iter) noexcept;

        std::unique_ptr<SimpleIterator<Result>>
        make_stream4num_r(const R * caller, const USR * src, Result prev_result) noexcept;

        std::unique_ptr<SimpleIterator<Result>>
        make_imatch_iter(const R * caller, const USR * src, Result prev_result) noexcept;
    }
}

#endif //LEVIDB_R_H