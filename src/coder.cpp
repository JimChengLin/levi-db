#include "coder.h"
#include "exception.h"

namespace LeviDB {
    void Holder::plus(int idx, int val) noexcept {
        total += val;
        while (true) {
            cum_cnt[idx] += val;
            idx += (idx & (-idx));
            if (idx > CoderConst::holder_size - 1) {
                break;
            }
        }
        if (total >= UINT16_MAX / 4) {
            halve();
        }
    }

    int Holder::getCum(int idx) const noexcept {
        int sum = cum_cnt[0];
        while (idx > 0) {
            sum += cum_cnt[idx];
            idx &= (idx - 1);
        }
        return sum;
    }

    void Holder::halve() noexcept {
        int gap[CoderConst::FN + 1];
        int lo = 0;
        for (int i = 0; i <= CoderConst::FN; ++i) {
            int hi = getCum(i + 1);
            gap[i] = hi - lo;
            lo = hi;
        }

        total = 0;
        memset(cum_cnt, 0, sizeof(cum_cnt));
        for (int i = 0; i <= CoderConst::FN; ++i) {
            if (gap[i] >= 1) {
                plus(i + 1, std::max(1, gap[i] / 2));
            }
        }
    }

    int Holder::findGreater(int cum) const noexcept {
        int lo = 0;
        int hi = CoderConst::FN + 1;
        while (lo < hi) {
            int mi = (lo + hi) / 2;
            if (cum < getCum(mi)) {
                hi = mi;
            } else {
                lo = mi + 1;
            }
        }
        return lo;
    }

    template<bool _>
    void SubCoder<_>::pushBit(bool bit, std::vector<uint8_t> & output, int & nth_bit_out) const noexcept {
        if (bit) {
            output.back() |= (1 << nth_bit_out);
        }
        if (--nth_bit_out < 0) {
            nth_bit_out = CHAR_BIT - 1;
            output.push_back(0);
        }
    }

    template<bool _>
    void SubCoder<_>::encode(int symbol, std::vector<uint8_t> & output, int & nth_bit_out) noexcept {
        assert(output.size() >= 1);
        assert(symbol >= 0 && symbol <= CoderConst::FN);
        assert(nth_bit_out >= 0 && nth_bit_out <= CHAR_BIT - 1);

        uint16_t o_lower = _lower;
        uint32_t o_range = static_cast<uint32_t>(_upper) - _lower + 1;

        _lower = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol) / _holder.total);
        _upper = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol + 1) / _holder.total - 1);

        while (condition_12() || condition_3()) {
            if (condition_12()) {
                auto bit = static_cast<bool>(_lower & mask_a);
                pushBit(bit, output, nth_bit_out);

                _lower <<= 1;
                _upper <<= 1;
                _upper |= 1;

                bit = !bit;
                while (_cnt_3) {
                    pushBit(bit, output, nth_bit_out);
                    --_cnt_3;
                }
            }

            if (condition_3()) {
                _lower <<= 1;
                _upper <<= 1;
                _upper |= 1;

                _lower ^= mask_a;
                _upper ^= mask_a;
                ++_cnt_3;
            }
        }
    };

    template<bool _>
    void SubCoder<_>::finishEncode(std::vector<uint8_t> & output, int & nth_bit_out) noexcept {
        _bit_q = _lower;
        pushBit(_bit_q[_bit_q.size() - 1], output, nth_bit_out);

        bool bit = !_bit_q[_bit_q.size() - 1];
        while (_cnt_3) {
            pushBit(bit, output, nth_bit_out);
            --_cnt_3;
        }

        for (auto i = static_cast<int>(_bit_q.size() - 1 - 1); i >= 0; --i) {
            pushBit(_bit_q[i], output, nth_bit_out);
        }
    }

    template<bool TRUE_NYT_FALSE_NORMAL>
    bool SubCoder<TRUE_NYT_FALSE_NORMAL>::fetchBit(const Slice & input, size_t & nth_byte_in,
                                                   int & nth_bit_in) const {
        bool bit;
        if (!TRUE_NYT_FALSE_NORMAL) { // normal forward
            if (nth_byte_in > input.size() - 1) {
                throw Exception::corruptionException("bad record length");
            }

            bit = static_cast<bool>(input[nth_byte_in] & (1 << nth_bit_in));
            if (--nth_bit_in < 0) {
                ++nth_byte_in;
                nth_bit_in = CHAR_BIT - 1;
            }
        } else { // NYT backward
            if (nth_bit_in < 0) {
                throw Exception::corruptionException("bad record length");
            }

            bit = static_cast<bool>(input[nth_byte_in] & (1 << nth_bit_in));
            if (--nth_bit_in < 0) {
                if (nth_byte_in >= 1) {
                    --nth_byte_in;
                    nth_bit_in = CHAR_BIT - 1;
                }
            }
        }
        return bit;
    }

    template<bool _>
    int SubCoder<_>::decode(const Slice & input, size_t & nth_byte_in, int & nth_bit_in) {
        if (_bit_q == _lower) {
            return CoderConst::decode_exit;
        }

        unsigned long anchor = ((_bit_q.to_ulong() - _lower + 1) * _holder.total - 1)
                               / (static_cast<uint32_t>(_upper) - _lower + 1);
        int symbol = _holder.findGreater(static_cast<int>(anchor)) - 1;

        uint16_t o_lower = _lower;
        uint32_t o_range = static_cast<uint32_t>(_upper) - _lower + 1;

        _lower = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol) / _holder.total);
        _upper = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol + 1) / _holder.total - 1);

        while (condition_12() || condition_3()) {
            if (condition_12()) {
                _lower <<= 1;
                _upper <<= 1;
                _upper |= 1;

                _bit_q <<= 1;
                _bit_q[0] = fetchBit(input, nth_byte_in, nth_bit_in);
            }

            if (condition_3()) {
                _lower <<= 1;
                _upper <<= 1;
                _upper |= 1;

                _bit_q <<= 1;
                _bit_q[0] = fetchBit(input, nth_byte_in, nth_bit_in);

                _lower ^= mask_a;
                _upper ^= mask_a;
                _bit_q ^= mask_a;
            }
        }

        return symbol;
    }

    template<bool TRUE_NYT_FALSE_NORMAL>
    void SubCoder<TRUE_NYT_FALSE_NORMAL>::initDecode(const Slice & input, size_t & nth_byte_in,
                                                     int & nth_bit_in) {
        nth_byte_in = (!TRUE_NYT_FALSE_NORMAL) ? 0 : (input.size() - 1);
        nth_bit_in = CHAR_BIT - 1;
        for (auto i = static_cast<int>(_bit_q.size() - 1); i >= 0; --i) {
            _bit_q[i] = fetchBit(input, nth_byte_in, nth_bit_in);
        }
    }

    template
    class SubCoder<true>;

    template
    class SubCoder<false>;

    std::vector<uint8_t> Coder::encode(const std::vector<int> & src) noexcept {
        int nth_bit_normal = CHAR_BIT - 1;
        int nth_bit_NYT = CHAR_BIT - 1;
        std::vector<uint8_t> output_normal(1);
        std::vector<uint8_t> output_NYT(1);

        for (int symbol:src) {
            if (isNew(symbol)) {
                // NYT
                _coder_normal.encode(CoderConst::NYT, output_normal, nth_bit_normal);
                // encode
                _coder_NYT.encode(symbol, output_NYT, nth_bit_NYT);
                // freq
                _coder_normal._holder.plus(symbol + 1, 1);
                _coder_NYT._holder.plus(symbol + 1, -1);
                // if no longer need NYT
                if (_coder_NYT._holder.total == 0) {
                    _coder_normal._holder.plus(CoderConst::NYT + 1, -1);
                }
            } else {
                // encode
                _coder_normal.encode(symbol, output_normal, nth_bit_normal);
                // freq
                _coder_normal._holder.plus(symbol + 1, 1);
            }
        }
        if (output_normal.size() == 1 && nth_bit_normal == CHAR_BIT - 1) { // single char src
        } else {
            _coder_normal.finishEncode(output_normal, nth_bit_normal);
        }
        _coder_NYT.finishEncode(output_NYT, nth_bit_NYT);

        output_normal.insert(output_normal.end(), output_NYT.crbegin(), output_NYT.crend());
        return output_normal;
    }

    std::vector<int> Coder::decode(const Slice & src) {
        int symbol;
        std::vector<int> res;
        int nth_bit_normal;
        int nth_bit_NYT;
        size_t nth_byte_normal;
        size_t nth_byte_NYT;

        _coder_normal.initDecode(src, nth_byte_normal, nth_bit_normal);
        _coder_NYT.initDecode(src, nth_byte_NYT, nth_bit_NYT);

        // get first symbol
        symbol = _coder_NYT.decode(src, nth_byte_NYT, nth_bit_NYT);
        res.push_back(symbol);
        if (nth_bit_NYT < 0) { // singe char src
            return res;
        }
        _coder_normal._holder.plus(symbol + 1, 1);
        _coder_NYT._holder.plus(symbol + 1, -1);

        size_t break_threshold = src.size() * CHAR_BIT;
        size_t iter_times = 0;
        while ((symbol = _coder_normal.decode(src, nth_byte_normal, nth_bit_normal)) != CoderConst::decode_exit
               && iter_times++ < break_threshold) {
            if (symbol == CoderConst::NYT) {
                symbol = _coder_NYT.decode(src, nth_byte_NYT, nth_bit_NYT);
                res.push_back(symbol);

                _coder_normal._holder.plus(symbol + 1, 1);
                _coder_NYT._holder.plus(symbol + 1, -1);
                if (_coder_NYT._holder.total == 0) {
                    _coder_normal._holder.plus(CoderConst::NYT + 1, -1);
                }
            } else {
                res.push_back(symbol);
                _coder_normal._holder.plus(symbol + 1, 1);
            }
        }

        if (_coder_NYT.decode(src, nth_byte_NYT, nth_bit_NYT) != CoderConst::decode_exit ||
            nth_byte_normal - nth_byte_NYT == 1) {
            throw Exception::corruptionException("record struct damaged");
        }

        return res;
    }

    bool Coder::isNew(int symbol) const noexcept {
        int lo = _coder_NYT._holder.getCum(symbol);
        int hi = _coder_NYT._holder.getCum(symbol + 1);
        return lo != hi;
    }
}