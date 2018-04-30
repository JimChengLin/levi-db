#include <thread>

#include "env.h"

#include "db_impl.h"
#include "filename.h"

namespace levidb {
    static constexpr char kAlloc[] = "_alloc";
    static constexpr char kRecycle[] = "_recycle";
    static constexpr char kClose[] = "close";
    static constexpr char kHardwareConcurrency[] = "hardware_concurrency";
    static constexpr char kSeq[] = "seq";

    DBImpl::DBImpl(const std::string & name,
                   const OpenOptions & options,
                   open_t)
            : name_(name.back() == '/' ? name : (name + '/')),
              options_(options),
              stores_(1),
              manager_(this),
              index_(OpenIndexes()) {
        LoadOrSetInitInfo();
    }

    DBImpl::DBImpl(const std::string & name,
                   const OpenOptions & options,
                   reopen_t)
            : name_(name.back() == '/' ? name : (name + '/')),
              options_(options),
              stores_(1),
              manager_(this),
              index_(ReopenIndexes()) {
        LoadOrSetInitInfo();
    }

    DBImpl::DBImpl(const std::string & name,
                   const OpenOptions & options,
                   repair_t) {
        assert(false);
    }

    DBImpl::~DBImpl() {
        size_t nth = 0;
        std::string temp;
        for (const auto & idx:index_.indexes_) {
            auto[alloc, recycle] = idx->AllocatorInfo();
            IndexFilename(nth++, name_, &temp);
            options_.manifestor->Set(temp + kAlloc, static_cast<int64_t>(alloc));
            options_.manifestor->Set(temp + kRecycle, recycle);
        }
        options_.manifestor->Set(kSeq, static_cast<int64_t>(UniqueSeq()));
        options_.manifestor->Set(kClose, 1);
    }

    bool DBImpl::Get(const Slice & k, std::string * v) const {
        return index_.Get(k, v);
    }

    std::unique_ptr<Iterator>
    DBImpl::GetIterator() const {
        return index_.GetIterator();
    }

    void DBImpl::Add(const Slice & k, const Slice & v) {
        index_.Add(k, v, true);
    }

    void DBImpl::Del(const Slice & k) {
        index_.Del(k);
    }

    bool DBImpl::Compact() {
        return false;
    }

    void DBImpl::Sync() {
        index_.Sync();
    }

    size_t DBImpl::GetLv(size_t seq) const {
        for (size_t i = 0; i < stores_.size(); ++i) {
            const auto & l = stores_[i];
            for (size_t s:l) {
                if (s == seq) {
                    return i;
                }
            }
        }
        assert(false);
        return 0;
    }

    bool DBImpl::IsCompressed(size_t seq) const {
        auto it = stores_map_.find(seq);
        if (it != stores_map_.cend()) {
            return it->second.compress;
        }
        return false;
    }

    size_t DBImpl::UniqueSeq() {
        return seq_.fetch_add(1);
    }

    void DBImpl::Register(size_t seq) {
        stores_[0].emplace_back(seq);
    }

    std::vector<std::unique_ptr<Index>>
    DBImpl::OpenIndexes() {
        std::string temp;
        std::vector<std::unique_ptr<Index>> result;
        int64_t hardware_concurrency = std::thread::hardware_concurrency();
        options_.manifestor->Set(kHardwareConcurrency, hardware_concurrency);
        for (size_t i = 0; i < hardware_concurrency; ++i) {
            IndexFilename(i, name_, &temp);
            result.emplace_back(Index::Open(temp, &manager_));
        }
        return result;
    }

    std::vector<std::unique_ptr<Index>>
    DBImpl::ReopenIndexes() {
        std::string temp;
        std::vector<std::unique_ptr<Index>> result;
        int64_t hardware_concurrency;
        options_.manifestor->Get(kHardwareConcurrency, &hardware_concurrency);
        for (size_t i = 0; i < hardware_concurrency; ++i) {
            IndexFilename(i, name_, &temp);
            int64_t alloc;
            int64_t recycle;
            options_.manifestor->Get(temp + kAlloc, &alloc);
            options_.manifestor->Get(temp + kRecycle, &recycle);
            result.emplace_back(Index::Reopen(temp, &manager_,
                                              static_cast<size_t>(alloc), recycle));
        }
        return result;
    }

    void DBImpl::LoadOrSetInitInfo() {
        int64_t seq = 0;
        options_.manifestor->Get(kSeq, &seq);
        seq_.store(static_cast<size_t>(seq));

        std::vector<std::string> children;
        penv::Env::Default()->GetChildren(name_, &children);
        for (const auto & child:children) {
            if (IsStore(child)) {
                size_t s = GetStoreSeq(child);
                size_t l = GetStoreLv(child);
                bool c = IsCompressedStore(child);
                if (stores_.size() <= l) {
                    stores_.resize(l + 1);
                }
                stores_[l].emplace_back(s);
                stores_map_.emplace(s, StoreInfo{c});
            }
        }
    }

    std::shared_ptr<DB>
    DB::Open(const std::string & name,
             const OpenOptions & options) {
        int64_t close = 0;
        options.manifestor->Get(kClose, &close);
        options.manifestor->Set(kClose, 0);
        if (penv::Env::Default()->FileExists(name)) {
            if (close) {
                return std::make_shared<DBImpl>(name, options, reopen_t());
            } else {
                return std::make_shared<DBImpl>(name, options, repair_t());
            }
        } else {
            penv::Env::Default()->CreateDir(name);
            return std::make_shared<DBImpl>(name, options, open_t());
        }
    }
}