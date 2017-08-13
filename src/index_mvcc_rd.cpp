#include "index_mvcc_rd.h"

namespace LeviDB {
    OffsetToData IndexMVCC::find(const Slice & k, uint64_t seq_num) const {

    }

    size_t IndexMVCC::size(uint64_t seq_num) const {

    }

    size_t IndexMVCC::size(const BDNode * node, uint64_t seq_num) const {

    }

    void IndexMVCC::insert(const Slice & k, OffsetToData v) {

    }

    void IndexMVCC::remove(const Slice & k) {

    }

    void IndexMVCC::sync() {

    }

    std::map<std::string/* k */, OffsetToData/* v */>
    IndexMVCC::pendingPart() const noexcept {

    }

    void IndexMVCC::applyPending() {

    }
}