#ifndef LEVIDB_CODER_H
#define LEVIDB_CODER_H

/*
 * 用于压缩的编码器
 */

#include "slice.h"
#include <climits>
#include <cstdint>

namespace LeviDB {
    enum SpecChar {
        NYT = UINT8_MAX + 1,
        FN = UINT8_MAX + 2,
    };

    struct Holder {
        static constexpr int length = FN + 1 + 1;
        int cum_cnt[length];

        constexpr Holder() noexcept : cum_cnt() {}

        void plus(int idx, int val) noexcept;

        int get_cum(int idx) const noexcept;

        int get_total() const noexcept { return get_cum(length - 1); }
    };

    struct HolderNYT : public Holder {
        constexpr HolderNYT() noexcept;
    };

    struct HolderNormal : public Holder {
        constexpr HolderNormal() noexcept;
    };

    class ArithmeticCoder {
    private:
        HolderNYT _holder_NYT;
        HolderNormal _holder_normal;

        uint16_t _lower;
        uint16_t _upper;

        static constexpr uint16_t mask_a = 1 << (sizeof(_lower) * CHAR_BIT - 1);
        static constexpr uint16_t mask_b = 1 << (sizeof(_lower) * CHAR_BIT - 1 - 1);

    public:
        ArithmeticCoder() noexcept
                : _holder_NYT(), _holder_normal(), _lower(0), _upper(UINT16_MAX) {}

        ~ArithmeticCoder() noexcept {}

        std::vector<uint8_t> encode(const Slice & source) noexcept;

        std::vector<uint8_t> decode(const Slice & source);

    private:
        inline bool condition_12() const noexcept {
            return static_cast<bool>(~(_lower ^ _upper) & mask_a);
        }

        inline bool condition_3() const noexcept {
            return !(_upper & mask_b) && (_lower & mask_b);
        }
    };
}

#endif //LEVIDB_CODER_H