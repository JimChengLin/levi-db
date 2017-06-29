#include "coder.h"
#include <bitset>

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
        for (int i = 1; i < CoderConst::holder_size; ++i) {
            cum_cnt[i] /= 2;
        }
        total = getCum(CoderConst::FN + 1);
    }

    template<bool _>
    void ArithmeticSubCoder<_>::pushBit(const bool bit, std::vector<uint8_t> & output, size_t & nth_byte_out,
                                        int & nth_bit_out) noexcept {
        if (bit) {
            output[nth_byte_out] |= (1 << nth_bit_out);
        }
        if (--nth_bit_out < 0) {
            ++nth_byte_out;
            nth_bit_out = CHAR_BIT - 1;
        }
    }

    template<bool _>
    void ArithmeticSubCoder<_>::encode(const int symbol, std::vector<uint8_t> & output, size_t & nth_byte_out,
                                       int & nth_bit_out) noexcept {
        assert(symbol >= 0);

        uint16_t o_lower = _lower;
        uint32_t o_range = static_cast<uint32_t>(_upper) - _lower + 1;

        _lower = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol) / _holder.total);
        _upper = o_lower + static_cast<uint16_t>(o_range * _holder.getCum(symbol + 1) / _holder.total - 1);

        while (condition_12() || condition_3()) {
            if (condition_12()) {
                bool bit = static_cast<bool>(_lower & mask_a);
                pushBit(bit, output, nth_byte_out, nth_bit_out);

                _lower <<= 1;
                _upper <<= 1;
                _upper |= 1;

                bit = !bit;
                while (_cnt_3) {
                    pushBit(bit, output, nth_byte_out, nth_bit_out);
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
    void ArithmeticSubCoder<_>::finishEncode(std::vector<uint8_t> & output, size_t & nth_byte_out,
                                             int & nth_bit_out) noexcept {
        auto residual = std::bitset<sizeof(_lower) * CHAR_BIT>(_lower);
        pushBit(residual[residual.size() - 1], output, nth_byte_out, nth_bit_out);

        bool bit = !residual[residual.size() - 1];
        while (_cnt_3) {
            pushBit(bit, output, nth_byte_out, nth_bit_out);
            --_cnt_3;
        }

        for (int i = static_cast<int>(residual.size() - 1 - 1); i >= 0; --i) {
            pushBit(residual[i], output, nth_byte_out, nth_bit_out);
        }
        return;
    }
}