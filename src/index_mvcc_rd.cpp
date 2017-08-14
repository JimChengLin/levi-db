#include "index_mvcc_rd.h"

namespace LeviDB {
    std::string IndexMVCC::find(const Slice & k, uint64_t seq_num) const {

    }

    void IndexMVCC::insert(const Slice & k, OffsetToData v) {

    }

    void IndexMVCC::remove(const Slice & k) {

    }

    void IndexMVCC::sync() {

    }

    // 将多个 history 虚拟合并成单一 iterator
    class MultiHistoryIterator : Iterator {

    };

    std::unique_ptr<Iterator> IndexMVCC::pendingPart(uint64_t seq_num) const noexcept {

    }

    void IndexMVCC::applyPending() {

    }

    // 真正的 Matcher 实现
    class MatcherOffsetImpl : Matcher {

    };

    std::unique_ptr<Matcher> IndexMVCC::offToMatcher(OffsetToData data) const noexcept {

    }

    class MatcherSliceImpl : Matcher {

    };

    std::unique_ptr<Matcher> IndexMVCC::sliceToMatcher(const Slice & slice) const noexcept {

    }
}