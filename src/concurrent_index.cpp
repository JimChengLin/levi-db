#include "concurrent_index.h"
#include "iterator_merger.h"

namespace levidb {
    bool ConcurrentIndex::Get(const Slice & k, std::string * v) const {
        return indexes_[Hash(k) % indexes_.size()]->Get(k, v);
    }

    bool ConcurrentIndex::GetInternal(const Slice & k, uint64_t * v) const {
        return indexes_[Hash(k) % indexes_.size()]->GetInternal(k, v);
    }

    bool ConcurrentIndex::Add(const Slice & k, const Slice & v, bool overwrite) {
        return indexes_[Hash(k) % indexes_.size()]->Add(k, v, overwrite);
    }

    bool ConcurrentIndex::AddInternal(const Slice & k, uint64_t v) {
        return indexes_[Hash(k) % indexes_.size()]->AddInternal(k, v);
    }

    bool ConcurrentIndex::Del(const Slice & k) {
        return indexes_[Hash(k) % indexes_.size()]->Del(k);
    }

    std::unique_ptr<Iterator>
    ConcurrentIndex::GetIterator() const {
        std::vector<std::unique_ptr<Iterator>> iters(indexes_.size());
        for (size_t i = 0; i < iters.size(); ++i) {
            iters[i] = indexes_[i]->GetIterator();
        }
        return std::make_unique<IteratorMerger>(std::move(iters));
    }

    void ConcurrentIndex::Sync() {
        for (auto & index:indexes_) {
            index->Sync();
        }
    }

    void ConcurrentIndex::RetireStore() {
        for (auto & index:indexes_) {
            index->RetireStore();
        }
    }

    // https://stackoverflow.com/questions/98153/whats-the-best-hashing-algorithm-to-use-on-a-stl-string-when-using-hash-map
    size_t ConcurrentIndex::Hash(const Slice & k) {
        size_t h = 0;
        for (size_t i = 0; i < k.size(); ++i) {
            h = h * 101 + k[i];
        }
        return h;
    }
}