/*
 * 由于作者在 EasyRegex 项目中的迭代器命名是小写+下划线
 * 这里为了转译方便, 保持统一
 * 这些迭代器 trivial 且不对外开放, 故不影响整体代码风格
 */

#include <functional>
#include <unordered_map>
#include <vector>

#include "r.h"
#include "state_machine.h"

namespace LeviDB {
    namespace Regex {
        typedef std::tuple<const uintptr_t, const Result> cache_key_t;
        typedef std::pair<std::unique_ptr<std::vector<Result>>, std::unique_ptr<SimpleIterator<Result>>> cache_value_t;

        struct CacheHasher {
            size_t operator()(const cache_key_t & key) const noexcept {
                Result result = std::get<1>(key);
                return std::hash<size_t>{}(std::get<0>(key)
                                           ^ result._op
                                           ^ result._ed
                                           ^ result._select_from
                                           ^ result._select_to
                                           ^ result._success);
            }
        };

        thread_local static std::unordered_map<cache_key_t, cache_value_t, CacheHasher> _cache;
        thread_local static bool _possible_mode = false;
        thread_local static Result _possible_result;

        class iter_base : public SimpleIterator<Result> {
        protected:
            const R * _caller;
            const USR * _src;
            const Result _prev_result;

            Result _result;
            int _line = 0;

        public:
            iter_base(const R * caller, const USR * src, Result prev_result) noexcept
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
            // coverity[generated_default_ctor_exception_spec_circularity]
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
                        _result = _prev_result;
                        YIELD();
                    }
                    if (_caller->_num_to == 0) {
                        RETURN();
                    }

                    counter = 0;
                    fa = _caller->_pattern_to.empty() ? std::make_unique<StateMachine>(_caller->_pattern)
                                                      : std::make_unique<RangeStateMachine>(_caller->_pattern,
                                                                                            _caller->_pattern_to);
                    fa->setResult(_prev_result);
                    for (i = std::max(_prev_result._ed, _prev_result._select_from);
                         i < std::min(_src->immut_src()->size(), static_cast<size_t>(_prev_result._select_to));
                         ++i) {
                        for (j = 7; j >= 0; --j) {
                            if (((_src->immut_extra()[i] >> j) & 1) == 0) {
                                _result = fa->send(UNKNOWN);
                            } else {
                                _result = fa->send(static_cast<Input>(((*_src->immut_src())[i] >> j) & 1));
                            }

                            if ((isPossibleMode() && i == _src->immut_src()->size() - 1 && j == 0)
                                && (_result.isContinue() || _result.isSuccess())) {
                                _possible_result = {_prev_result._op,
                                                    static_cast<int>(_src->immut_src()->size()),
                                                    true};
                                _result = {};
                            }

                            if (_result.isContinue()) {
                            } else if (_result.isSuccess()) {
                                ++counter;
                                if (_caller->_num_from <= counter && counter <= _caller->_num_to) {
                                    YIELD();
                                }
                                if (counter < _caller->_num_to) {
                                    fa->reset();
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
        make_stream4num_machine(const R * caller, const USR * src, Result prev_result) noexcept {
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

        // 复刻 Python 的 list comprehension
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
            list_comprehension(IN_ITER && in, cond_t cond, trans_t trans) noexcept
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
                    _cursor = _chain_it->item();
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
                if (!_cursor->valid()) {
                    _chain_it->next();
                    if (_chain_it->valid()) {
                        _cursor = _chain_it->item();
                    }
                }
            }

            Result item() const override { return _cursor->item(); }
        };

        class stream4num_r : public iter_base {
        private:
            std::vector<std::tuple<std::unique_ptr<SimpleIterator<Result>>, int, Result>> q;
            int counter{};

        public:
            // coverity[generated_default_ctor_exception_spec_circularity]
            using iter_base::iter_base;
            DELETE_MOVE(stream4num_r);
            DELETE_COPY(stream4num_r);

        public:
            ~stream4num_r() noexcept override = default;

            void next() override {
                typedef std::unique_ptr<SimpleIterator<Result>> in_t;
                typedef std::unique_ptr<SimpleIterator<Result>> out_t;

                std::unique_ptr<SimpleIterator<Result>> curr_iter;
                int nth{};

                auto cond_func = [](const Result & result) noexcept -> bool {
                    return !result.isContinue() && result.isSuccess();
                };

                auto trans_func = [&](const Result & result) noexcept -> out_t {
                    return _caller->_r->imatch(*_src, result);
                };

                GEN_INIT();
                    if (_caller->_num_to == 0) {
                        _result = _prev_result;
                        YIELD();
                        RETURN();
                    }
                    if (_caller->_mode == LAZY && _caller->_num_from == 0) {
                        _result = _prev_result;
                        YIELD()
                    }

                    counter = 1;
                    curr_iter = _caller->_r->imatch(*_src, _prev_result);
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

                        if (curr_iter != nullptr && curr_iter->valid()) {
                            _result = curr_iter->item();
                            curr_iter->next();
                            std::get<0>(q.back()) = std::move(curr_iter); // we will use it later

                            if (_result.isContinue()) {
                            } else {
                                nth = std::get<1>(q.back());
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
                        } else {
                            _result = std::get<2>(q.back());
                            if (!_result.isContinue()) {
                                YIELD();
                            }
                            q.pop_back();
                        }
                    }

                    if (_caller->_mode == GREEDY && _caller->_num_from == 0) {
                        _result = _prev_result;
                        YIELD();
                    }
                GEN_STOP();
            }
        };

        std::unique_ptr<SimpleIterator<Result>>
        make_stream4num_r(const R * caller, const USR * src, Result prev_result) noexcept {
            return std::make_unique<stream4num_r>(caller, src, prev_result);
        };

        class logic_iter : public iter_base {
        protected:
            std::unique_ptr<SimpleIterator<Result>> _stream4num;

        public:
            logic_iter(const R * caller, const USR * src, Result prev_result,
                       std::unique_ptr<SimpleIterator<Result>> && stream4num) noexcept
                    : iter_base(caller, src, prev_result), _stream4num(std::move(stream4num)) {}
        };

        class and_r_iter : public logic_iter {
        private:
            std::unique_ptr<SimpleIterator<Result>> other_stream;

        public:
            using logic_iter::logic_iter;
            DELETE_MOVE(and_r_iter);
            DELETE_COPY(and_r_iter);

            ~and_r_iter() noexcept override = default;

            void next() override {
                Result echo;
                Result and_echo;

                GEN_INIT();
                    while (_stream4num->valid()) {
                        echo = _stream4num->item();
                        if (echo.isContinue() || echo.isFail()) {
                            _result = echo;
                        } else {
                            echo.asFail();
                            other_stream = _caller->_other->imatch(*_src, Result(0, 0, false),
                                                                   _prev_result._ed, echo._ed);
                            while (other_stream->valid()) {
                                and_echo = other_stream->item();
                                if (!and_echo.isContinue()
                                    && and_echo.isSuccess() && and_echo._ed == echo._ed - _prev_result._ed) {
                                    echo.asSuccess();
                                    break;
                                }
                                other_stream->next();
                            }
                            _result = echo;
                        }
                        YIELD();
                        _stream4num->next();
                    }
                GEN_STOP();
            }
        };

        class or_r_iter : public logic_iter {
        private:
            std::unique_ptr<SimpleIterator<Result>> other_stream;

        public:
            using logic_iter::logic_iter;
            DELETE_MOVE(or_r_iter);
            DELETE_COPY(or_r_iter);

            ~or_r_iter() noexcept override = default;

            void next() override {
                GEN_INIT();
                    while (_stream4num->valid()) {
                        _result = _stream4num->item();
                        YIELD();
                        _stream4num->next();
                    }

                    other_stream = _caller->_other->imatch(*_src, _prev_result);
                    while (other_stream->valid()) {
                        _result = other_stream->item();
                        YIELD();
                        other_stream->next();
                    }
                GEN_STOP();
            }
        };

        class invert_r_iter : public logic_iter {
        public:
            using logic_iter::logic_iter;
            DELETE_MOVE(invert_r_iter);
            DELETE_COPY(invert_r_iter);

            ~invert_r_iter() noexcept override = default;

            void next() override {
                GEN_INIT();
                    while (_stream4num->valid()) {
                        _result = _stream4num->item().invert();
                        YIELD();
                        /*
                         * 预期: fail 的 match, invert 后成为 success
                         * 但由于 possible mode, 原本 fail 的 match 直接就是 success, 再 invert 就变成 fail, 不符合预期
                         * 解决方案: 两种结果都 yield
                         */
                        if (isPossibleMode()) {
                            _result.invert();
                            YIELD();
                        }
                        _stream4num->next();
                    }
                GEN_STOP();
            }
        };

        class next_r_iter : public logic_iter {
        private:
            std::unique_ptr<SimpleIterator<Result>> concat_iter;

        public:
            using logic_iter::logic_iter;
            DELETE_MOVE(next_r_iter);
            DELETE_COPY(next_r_iter);

            ~next_r_iter() noexcept override = default;

            void next() override {
                typedef std::unique_ptr<SimpleIterator<Result>> in_t;
                typedef std::unique_ptr<SimpleIterator<Result>> out_t;

                auto cond_func = [](const Result & result) noexcept -> bool {
                    return !result.isContinue() && result.isSuccess();
                };

                auto trans_func = [&](const Result & result) noexcept -> out_t {
                    return _caller->_other->imatch(*_src, result);
                };

                GEN_INIT();
                    // @formatter:off
                    concat_iter = std::make_unique<chain_from_iterable>(
                            std::make_unique<list_comprehension<in_t, out_t>>(std::move(_stream4num),
                                                                              cond_func, trans_func));
                    // @formatter:on

                    while (concat_iter->valid()) {
                        _result = concat_iter->item();
                        YIELD();
                        concat_iter->next();
                    }
                GEN_STOP();
            }
        };

        class imatch_iter : public iter_base {
        private:
            std::unique_ptr<SimpleIterator<Result>> merge_iter;

        public:
            using iter_base::iter_base;
            DELETE_MOVE(imatch_iter);
            DELETE_COPY(imatch_iter);

        public:
            ~imatch_iter() noexcept override = default;

#define BASE_ARGS _caller, _src, _prev_result

            void next() override {
                GEN_INIT();
                    {
                        std::unique_ptr<SimpleIterator<Result>> stream4num;
                        if (!_caller->_pattern.empty()) {
                            stream4num = std::make_unique<stream4num_machine>(BASE_ARGS);
                            if (_caller->_mode == LAZY) {
                                stream4num = std::make_unique<reversed>(std::move(stream4num));
                            }
                        } else {
                            stream4num = std::make_unique<stream4num_r>(BASE_ARGS);
                        }

                        std::unique_ptr<SimpleIterator<Result>> stream4logic;
                        switch (_caller->_relation) {
                            case R::AND:
                                stream4logic = std::make_unique<and_r_iter>(BASE_ARGS, std::move(stream4num));
                                break;

                            case R::OR:
                                stream4logic = std::make_unique<or_r_iter>(BASE_ARGS, std::move(stream4num));
                                break;

                            case R::INVERT:
                                stream4logic = std::make_unique<invert_r_iter>(BASE_ARGS, std::move(stream4num));
                                break;

                            case R::NEXT:
                                stream4logic = std::make_unique<next_r_iter>(BASE_ARGS, std::move(stream4num));
                                break;

                            case R::NONE:
                                stream4logic = std::move(stream4num);
                                break;

                            case R::XOR:
                            default:
                                assert(false);
                        }

                        merge_iter = std::move(stream4logic);
                    }

                    while (merge_iter->valid()) {
                        _result = merge_iter->item();
                        YIELD();
                        merge_iter->next();
                    }
                GEN_STOP();
            }
        };

        class imatch_iter_wrapper : public SimpleIterator<Result> {
        private:
            std::vector<Result> * _product;
            SimpleIterator<Result> * _producer;
            int i = -1;

        public:
            imatch_iter_wrapper(const R * caller, const USR * src, Result prev_result) noexcept {
                auto & pair = _cache[{reinterpret_cast<uintptr_t>(caller), prev_result}];
                if (pair.first == nullptr) {
                    assert(pair.second == nullptr);
                    pair.first = std::make_unique<std::vector<Result>>();
                    pair.second = std::make_unique<imatch_iter>(caller, src, prev_result);
                }
                _product = pair.first.get();
                _producer = pair.second.get();
                next();
            }

            DELETE_MOVE(imatch_iter_wrapper);
            DELETE_COPY(imatch_iter_wrapper);

        public:
            ~imatch_iter_wrapper() noexcept override = default;

            bool valid() const override {
                return i != -1 && i < _product->size();
            }

            void next() noexcept override {
                if (i + 1 < _product->size()) {
                    ++i;
                } else if (_producer->valid()) {
                    _product->emplace_back(_producer->item());
                    _producer->next();
                    ++i;

                    if (_product->back().isContinue()) {
                        _product->pop_back();
                        --i;
                        return next();
                    }
                } else if (i < _product->size()) { // valid => invalid
                    ++i;
                }
            }

            Result item() const override {
                return (*_product)[i];
            }
        };

        std::unique_ptr<SimpleIterator<Result>>
        make_imatch_iter(const R * caller, const USR * src, Result prev_result) noexcept {
            assert(_cache.empty()); // clean test
            return std::make_unique<imatch_iter>(caller, src, prev_result);
        };

        std::unique_ptr<SimpleIterator<Result>>
        R::imatch(const USR & input, Result prev_result) const noexcept {
            return std::make_unique<imatch_iter_wrapper>(this, &input, prev_result);
        }

        std::unique_ptr<SimpleIterator<Result>>
        R::imatch(const USR & input, Result prev_result, int from, int to) const noexcept {
            prev_result._select_from = from;
            prev_result._select_to = to;
            return std::make_unique<imatch_iter_wrapper>(this, &input, prev_result);
        }

        void enablePossibleMode() noexcept {
            _possible_mode = true;
        }

        void disablePossibleMode() noexcept {
            _possible_mode = false;
        }

        bool isPossibleMode() noexcept {
            return _possible_mode;
        }

        void cacheClear() noexcept {
            _cache.clear();
        }

        Result & getPossibleResultRef() noexcept {
            return _possible_result;
        }
    }
}