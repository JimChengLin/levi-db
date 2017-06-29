#include "coder.h"

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
}