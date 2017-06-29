#ifndef LEVIDB_CODER_H
#define LEVIDB_CODER_H

/*
 * 用于压缩的编码器
 */

#include <cstdint>

namespace LeviDB {
    enum SpecChar {
        NYT = UINT8_MAX + 1,
        FN = UINT8_MAX + 2,
    };

    struct Holder {
        int cum_cnt[FN + 1 + 1];

        constexpr Holder() noexcept : cum_cnt() {}

        void plus(int idx, int val) noexcept;

        int get_cum(int idx) const noexcept;
    };

    struct HolderNYT : public Holder {
        constexpr HolderNYT() noexcept;
    };

    struct HolderNormal : public Holder {
        constexpr HolderNormal() noexcept;
    };
}

#endif //LEVIDB_CODER_H