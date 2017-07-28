#ifndef LEVIDB_CODER_H
#define LEVIDB_CODER_H

/*
 * 用于压缩的算术编码器
 */

#include "slice.h"
#include <bitset>
#include <climits>
#include <cstdint>
#include <vector>

namespace LeviDB {
    namespace CoderConst {
        enum SpecChar {
            NYT = UINT8_MAX + 1,
            FN = UINT8_MAX + 2,
        };
        static constexpr int holder_size = FN + 1 + 1;
        static constexpr int decode_exit = -1;
    }

    struct HolderNYT {
        int cum_cnt[CoderConst::holder_size] = {};
        int total = 0;

        constexpr HolderNYT() noexcept {
            for (int i = 0; i <= CoderConst::FN; ++i) {
                if (i != CoderConst::NYT) {
                    // plus
                    int idx = i + 1;
                    constexpr int val = 1;
                    while (true) {
                        cum_cnt[idx] += val;
                        idx += (idx & (-idx));
                        if (idx > CoderConst::holder_size - 1) {
                            break;
                        }
                    }
                    // ---
                    ++total;
                }
            }
        };
    };

    struct HolderNormal {
        int cum_cnt[CoderConst::holder_size] = {};
        int total = 0;

        constexpr HolderNormal() noexcept {
            // plus
            int idx = CoderConst::NYT + 1;
            constexpr int val = 1;
            while (true) {
                cum_cnt[idx] += val;
                idx += (idx & (-idx));
                if (idx > CoderConst::holder_size - 1) {
                    break;
                }
            }
            // ---
            ++total;
        };
    };

    static constexpr HolderNYT holderNYT{};
    static constexpr HolderNormal holderNormal{};

    struct Holder {
        int cum_cnt[CoderConst::holder_size] = {};
        int total = 0;

        int getCum(int idx) const noexcept;

        void plus(int idx, int val) noexcept;

        int findGreater(int cum) const noexcept;

        void halve() noexcept;
    };

    class Coder;

    template<bool TRUE_NYT_FALSE_NORMAL>
    class SubCoder {
    private:
        Holder _holder;
        std::bitset<sizeof(uint16_t) * CHAR_BIT> _bit_q;
        int _cnt_3 = 0;
        uint16_t _lower = 0;
        uint16_t _upper = UINT16_MAX;

        friend class Coder;

        static constexpr uint16_t mask_a = 1 << (sizeof(_lower) * CHAR_BIT - 1);
        static constexpr uint16_t mask_b = 1 << (sizeof(_lower) * CHAR_BIT - 1 - 1);

    public:
        SubCoder() noexcept
                : _holder(TRUE_NYT_FALSE_NORMAL ? *reinterpret_cast<const Holder *>(&holderNYT)
                                                : *reinterpret_cast<const Holder *>(&holderNormal)) {}

        ~SubCoder() noexcept = default;

        // 0-based nth, 开始时 output.size() == 1
        void encode(int symbol, std::vector<uint8_t> & output, int & nth_bit_out) noexcept;

        void finishEncode(std::vector<uint8_t> & output, int & nth_bit_out) noexcept;

        // NYT 与 normal 在 decode 时有差异: normal 从前往后, NYT 相反
        int decode(const Slice & input, size_t & nth_byte_in, int & nth_bit_in);

        void initDecode(const Slice & input, size_t & nth_byte_in, int & nth_bit_in);

    private:
        inline void pushBit(bool bit, std::vector<uint8_t> & output, int & nth_bit_out) const noexcept;

        inline bool fetchBit(const Slice & input, size_t & nth_byte_in, int & nth_bit_in) const;

        inline bool condition_12() const noexcept {
            return static_cast<bool>(~(_lower ^ _upper) & mask_a);
        }

        inline bool condition_3() const noexcept {
            return !(_upper & mask_b) && (_lower & mask_b);
        }
    };

    typedef SubCoder<true> SubCoderNYT;
    typedef SubCoder<false> SubCoderNormal;

    class Coder {
    private:
        SubCoderNYT _coder_NYT;
        SubCoderNormal _coder_normal;

    public:
        Coder() noexcept = default;

        ~Coder() noexcept = default;

        std::vector<uint8_t> encode(const std::vector<int> & src) noexcept;

        std::vector<int> decode(const Slice & src);

    private:
        bool isNew(int symbol) const noexcept;
    };
}

#endif //LEVIDB_CODER_H