#ifndef LEVIDB_CODER_H
#define LEVIDB_CODER_H

/*
 * 用于压缩的算术编码器
 */

#include "slice.h"
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
    }

    struct HolderNYT {
        int cum_cnt[CoderConst::holder_size];
        int total;

        constexpr HolderNYT() noexcept : cum_cnt(), total(0) {
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
                }
            }
        };
    };

    struct HolderNormal {
        int cum_cnt[CoderConst::holder_size];
        int total;

        constexpr HolderNormal() noexcept : cum_cnt(), total(0) {
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
        };
    };

    static constexpr HolderNYT holderNYT{};
    static constexpr HolderNormal holderNormal{};

    struct Holder {
        int cum_cnt[CoderConst::holder_size];
        int total;

        int getCum(int idx) const noexcept;

        void plus(int idx, int val) noexcept;

    private:
        void halve() noexcept;
    };

    template<bool TRUE_NYT_FALSE_NORMAL>
    class ArithmeticSubCoder {
    private:
        Holder _holder;

        uint16_t _lower;
        uint16_t _upper;

        static constexpr uint16_t mask_a = 1 << (sizeof(_lower) * CHAR_BIT - 1);
        static constexpr uint16_t mask_b = 1 << (sizeof(_lower) * CHAR_BIT - 1 - 1);

    public:
        ArithmeticSubCoder() noexcept
                : _holder(TRUE_NYT_FALSE_NORMAL ? *reinterpret_cast<Holder *>(const_cast<HolderNYT *>(&holderNYT))
                                                : *reinterpret_cast<Holder *>(const_cast<HolderNormal *>(&holderNormal))),
                  _lower(0),
                  _upper(UINT16_MAX) {}

        ~ArithmeticSubCoder() noexcept {}

    private:
        inline bool condition_12() const noexcept {
            return static_cast<bool>(~(_lower ^ _upper) & mask_a);
        }

        inline bool condition_3() const noexcept {
            return !(_upper & mask_b) && (_lower & mask_b);
        }
    };

    typedef ArithmeticSubCoder<true> ArithmeticNYTCoder;
    typedef ArithmeticSubCoder<false> ArithmeticNormalCoder;
}

#endif //LEVIDB_CODER_H