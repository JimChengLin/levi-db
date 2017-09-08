/*
 * 由于作者在 EasyRegex 项目中的迭代器命名是小写+下划线
 * 这里为了转译方便, 保持统一
 * 这些迭代器 trivial 且不对外开放, 故不影响整体代码风格
 */

#include <functional>
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
                                _result = fa->send(
                                        static_cast<Input>(((*_src->immut_src())[i] >> j) & 1));
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

        std::unique_ptr<SimpleIterator<Result>>
        make_stream4num_machine(const R * caller, const USR * src, const Result * prev_result) noexcept {
            return std::make_unique<stream4num_machine>(caller, src, prev_result);
        };

        class reversed : public SimpleIterator<Result> {
        private:
            std::vector<Result> _results;
            std::vector<Result>::const_iterator _cursor;

        public:
            explicit reversed(std::unique_ptr<SimpleIterator<Result>> && result_iter) noexcept {
                while (result_iter->valid()) {
                    _results.emplace_back(result_iter->item());
                    result_iter->next();
                }
                _cursor = _results.empty() ? _results.cend() : --_results.cend();
            }

            DELETE_MOVE(reversed);
            DELETE_COPY(reversed);

        public:
            ~reversed() noexcept override = default;

            bool valid() const override { return _cursor != _results.cend(); }

            void next() override {
                if (_cursor == _results.cbegin()) {
                    _cursor = _results.cend();
                } else {
                    --_cursor;
                }
            }

            Result item() const override { return *_cursor; }
        };

        std::unique_ptr<SimpleIterator<Result>>
        make_reversed(std::unique_ptr<SimpleIterator<Result>> && result_iter) noexcept {
            return std::make_unique<reversed>(std::move(result_iter));
        };

        template<typename IN_ITER, typename OUT_ITEM>
        class list_comprehension : public SimpleIterator<OUT_ITEM> {
        private:
            typedef decltype(&IN_ITER::element_type::item) func_t;
            typedef typename std::result_of<func_t(IN_ITER)>::type item_t;

            typedef std::function<bool(item_t)> cond_t;
            typedef std::function<OUT_ITEM(item_t)> trans_t;

            IN_ITER _in;
            cond_t _cond;
            trans_t _trans;

        public:
            explicit list_comprehension(IN_ITER && in, cond_t cond, trans_t trans) noexcept
                    : _in(std::forward<IN_ITER>(in)), _cond(std::move(cond)), _trans(std::move(trans)) {
                if (list_comprehension::valid() && !_cond(_in->item())) {
                    list_comprehension::next();
                }
            }
            DELETE_MOVE(list_comprehension);
            DELETE_COPY(list_comprehension);

        public:
            ~list_comprehension() noexcept override = default;

            bool valid() const override { return _in->valid(); }

            void next() override {
                do {
                    _in->next();
                } while (_in->valid() && !_cond(_in->item()));
            }

            OUT_ITEM item() const override { return _trans(_in->item()); }
        };

        // 复刻 Python 的 chain.from_iterable
        // 把多个 iterable 合并成一个
        class chain_from_iterable : public SimpleIterator<Result> {
        private:
            typedef std::unique_ptr<SimpleIterator<Result>> iterable_t;
            typedef std::unique_ptr<SimpleIterator<iterable_t>> chain_t;

            chain_t _chain_it;
            iterable_t _cursor;

        public:
            explicit chain_from_iterable(chain_t chain_it) noexcept
                    : _chain_it(std::move(chain_it)) {
                if (_chain_it->valid()) {
                    _cursor = chain_it->item();
                }
            }
            DELETE_MOVE(chain_from_iterable);
            DELETE_COPY(chain_from_iterable);

        public:
            ~chain_from_iterable() noexcept override = default;

            bool valid() const override {
                return _cursor != nullptr && _cursor->valid();
            }

            void next() override {
                _cursor->next();
                if (!_cursor->valid() && _chain_it->valid()) {
                    _chain_it->next();
                    _cursor = _chain_it->item();
                }
            }

            Result item() const override { return _cursor->item(); }
        };

        class stream4num_r : public iter_base {
        private:
            std::vector<std::tuple<std::unique_ptr<SimpleIterator<Result>>, int, Result>> q;
            int counter{};

        public:
            using iter_base::iter_base;
            DELETE_MOVE(stream4num_r);
            DELETE_COPY(stream4num_r);

        public:
            ~stream4num_r() noexcept override = default;

            void next() override {
                std::unique_ptr<SimpleIterator<Result>> curr_iter;
                int nth{};

                typedef decltype(curr_iter) in_t;
                typedef decltype(curr_iter) out_t;

                auto cond_func = [](const Result & result) -> bool {
                    return result.isSuccess();
                };

                auto trans_func = [&](const Result & result) -> out_t {
                    return _caller->_r->imatch(*_src, result);
                };

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
                    while (counter < _caller->_num_from) {
                        ++counter;
                        // @formatter:off
                        curr_iter = std::make_unique<chain_from_iterable>(
                                std::make_unique<list_comprehension<in_t, out_t>>(std::move(curr_iter),
                                                                                  cond_func, trans_func));
                        // @formatter:on
                    }

                    q.emplace_back(std::move(curr_iter), counter, Result{});
                    while (!q.empty()) {
                        curr_iter = std::move(std::get<0>(q.back()));
                        nth = std::get<1>(q.back());

                        curr_iter->next();
                        if (curr_iter->valid()) {
                            _result = curr_iter->item();

                            if (_result.isContinue()) {
                            } else {
                                if (_caller->_mode == LAZY) {
                                    if (_result.isSuccess() && nth < _caller->_num_to) {
                                        q.emplace_back(_caller->_r->imatch(*_src, _result), nth + 1, Result{});
                                    }
                                    YIELD();
                                } else {
                                    if (_result.isSuccess() && nth < _caller->_num_to) {
                                        q.emplace_back(_caller->_r->imatch(*_src, _result), nth + 1, _result);
                                    } else {
                                        YIELD();
                                    }
                                }
                            }

                            std::get<0>(q.back()) = std::move(curr_iter); // give back ownership
                        } else {
                            _result = std::get<2>(q.back());
                            if (!_result.isContinue()) {
                                YIELD();
                            }
                            q.pop_back();
                        }
                    }

                    if (_caller->_mode == GREEDY && _caller->_num_from == 0) {
                        _result = *_prev_result;
                        YIELD();
                    }
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

            void next() override {}
        };

        std::unique_ptr<SimpleIterator<Result>>
        R::imatch(const USR & input, Result prev_result) const noexcept {
            return std::make_unique<imatch_iter>(this, &input, &prev_result);
        }
    }
}