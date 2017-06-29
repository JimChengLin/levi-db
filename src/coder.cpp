#include "coder.h"

namespace LeviDB {
    static constexpr int table_size = FN + 1 + 1;

    void Holder::plus(int idx, int val) noexcept {
        while (true) {
            cum_cnt[idx] += val;
            idx += (idx & (-idx));
            if (idx > table_size - 1) {
                break;
            }
        }
    }

    int Holder::get_cum(int idx) const noexcept {
        int sum = cum_cnt[0];
        while (idx > 0) {
            sum += cum_cnt[idx];
            idx &= (idx - 1);
        }
        return sum;
    }

    constexpr HolderNYT::HolderNYT() noexcept : Holder() {
        for (int i = 0; i <= FN; ++i) {
            if (i != NYT) {
                int idx = i + 1;
                constexpr int val = 1;
                while (true) {
                    cum_cnt[idx] += val;
                    idx += (idx & (-idx));
                    if (idx > table_size - 1) {
                        break;
                    }
                }
            }
        }
    }

    constexpr HolderNormal::HolderNormal() noexcept : Holder() {
        int idx = NYT + 1;
        constexpr int val = 1;
        while (true) {
            cum_cnt[idx] += val;
            idx += (idx & (-idx));
            if (idx > table_size - 1) {
                break;
            }
        }
    }
}