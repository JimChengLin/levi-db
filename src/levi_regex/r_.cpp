/*
 * 由于作者在 EasyRegex 项目中迭代器命名是小写+下划线
 * 这里为了转译方便, 保持统一
 */

#include <vector>

#include "r.h"
#include "state_machine.h"

namespace LeviDB {
    namespace Regex {
        class iter_base : public SimpleIterator<Result> {
        protected:
            const R * _caller;
            const USR * _src;
            const Result * _prev_result;

            Result _result;
            int _line = 0;

        public:
            iter_base(const R * caller, const USR * src, const Result * prev_result) noexcept
                    : _caller(caller), _src(src), _prev_result(prev_result) {}

            bool valid() const override { return _line != -1; }

            Result item() const override { return _result; }
        };

        class stream4num_machine : public iter_base {
        private:
            std::unique_ptr<StateMachine> fa;
            int counter{};
            int i{};
            int j{};

        public:
            using iter_base::iter_base;
            DELETE_MOVE(stream4num_machine);
            DELETE_COPY(stream4num_machine);

        public:
            ~stream4num_machine() noexcept override = default;

#define GEN_INIT() switch(_line) { case 0:;
#define YIELD() _line = __LINE__; return; case __LINE__:;
#define GEN_STOP() default:; } _line = -1;
#define RETURN() _line = -1; return;

            void next() override {
                GEN_INIT();
                    if (_caller->_num_from == 0) {
                        _result = *_prev_result;
                        YIELD();
                    }
                    if (_caller->_num_to == 0) {
                        RETURN();
                    }

                    counter = 0;
                    fa = std::make_unique<StateMachine>(_caller->_pattern);
                    fa->setResult(*_prev_result);
                    for (i = _prev_result->_ed; i < _src->immut_src()->size(); ++i) {
                        for (j = 7; j >= 0; --j) {
                            if (((_src->immut_extra()[i] >> j) & 1) == 0) {
                                _result = fa->send(UNKNOWN);
                            } else {
                                _result = fa->send(static_cast<Input>(((*_src->immut_src())[i] >> j) & 1));
                            }

                            if (_result.isContinue()) {
                            } else if (_result.isSuccess()) {
                                ++counter;
                                if (_caller->_num_from <= counter && counter <= _caller->_num_to) {
                                    YIELD();
                                }
                                if (counter < _caller->_num_to) {
                                    fa = std::make_unique<StateMachine>(_caller->_pattern);
                                    fa->setResult(_result);
                                } else {
                                    RETURN();
                                }
                            } else {
                                YIELD();
                                RETURN();
                            }
                        }
                    }
                GEN_STOP();
            }
        };

        class reversed : public SimpleIterator<Result> {
        private:
            std::vector<Result> _results;
            std::vector<Result>::const_iterator _cursor;

        public:
            reversed(std::unique_ptr<SimpleIterator<Result>> result_iter) noexcept {

            }

            bool valid() const override {

            }

            void next() override {

            }

            Result item() const override {

            }
        };

        class chain_from_iterable : SimpleIterator<Result> {

        };

        class stream4num_r : public iter_base {
        private:
            size_t counter{};
            std::unique_ptr<SimpleIterator<Result>> curr_iter;
        public:
            using iter_base::iter_base;
            DELETE_MOVE(stream4num_r);
            DELETE_COPY(stream4num_r);

        public:
            ~stream4num_r() noexcept override = default;

            void next() override {
                GEN_INIT();
                    if (_caller->_num_to == 0) {
                        _result = *_prev_result;
                        YIELD();
                        RETURN();
                    }
                    if (_caller->_mode == LAZY && _caller->_num_from == 0) {
                        _result = *_prev_result;
                        YIELD()
                    }

                    counter = 1;
                    curr_iter = _caller->_r->imatch(*_src, *_prev_result);

                GEN_STOP();
            }
        };

        class imatch_iter : public iter_base {
        public:
            using iter_base::iter_base;
            DEFAULT_MOVE(imatch_iter);
            DEFAULT_COPY(imatch_iter);

        public:
            ~imatch_iter() noexcept override = default;

            void next() override {

            }
        };

        std::unique_ptr<SimpleIterator<Result>>
        R::imatch(const USR & input, Result prev_result) const noexcept {
            return std::make_unique<imatch_iter>(this, &input, &prev_result);
        }
    }
}