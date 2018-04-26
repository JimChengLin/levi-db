#include "../include/db.h"
#include "db_impl.h"

namespace levidb {
    size_t DBImpl::GetLv(size_t seq) const {

    }

    bool DBImpl::IsCompressed(size_t seq) const {

    }

    size_t DBImpl::UniqueSeq() {

    }

    void DBImpl::RegisterStore(size_t seq) {

    }

    std::shared_ptr<DB>
    DB::Open(const std::string & name,
             const levidb::OpenOptions & options) {

    }
}