#include "db_impl.h"
#include "filename.h"
#include "store_manager.h"

namespace levidb {
    std::shared_ptr<Store>
    StoreManager::OpenStoreForRandomRead(size_t seq) {
        std::lock_guard guard(mutex_);
        std::shared_ptr<Store> result;
        if (cache_.Get(seq, &result)) {
        } else {
            StoreFilename(seq, db_->GetLv(seq), db_->IsCompressed(seq), db_->GetName(), &backup_);
            result = Store::OpenForRandomRead(backup_);
            cache_.Add(seq, result);
        }
        return result;
    }

    std::shared_ptr<Store>
    StoreManager::OpenStoreForReadWrite(size_t * seq, std::shared_ptr<levidb::Store> prev) {
        std::lock_guard guard(mutex_);
        if (curr_ == nullptr || prev == curr_) {
            seq_ = db_->UniqueSeq();
            StoreFilename(seq_, 0, false, db_->GetName(), &backup_);
            curr_ = Store::OpenForReadWrite(backup_);
            db_->Register(seq_);
        }
        *seq = seq_;
        return curr_;
    }
}