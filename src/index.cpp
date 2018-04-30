/*
 * token(64bits) = sign(1bit) + seq(31bits) + padding(1bit) + id(31bits)
 * kv = k_len(varint32) + k(char[]) + v(char[])
 * k_len == 0 -> del
 *
 * sign == 0 -> kv
 *      == 1 -> node(offset/kPageSize)
 */

#include "coding.h"
#include "env.h"
#include "sig_tree_impl.h"
#include "sig_tree_iter_impl.h"
#include "sig_tree_node_impl.h"

#include "index.h"
#include "index_format.h"

namespace levidb {
    class Helper;

    class IndexImpl;

    class KVTrans {
    private:
        Helper * helper_;
        uint64_t & rep_;
        uint32_t k_len_;
        logream::Slice s_;

    public:
        KVTrans(Helper * helper, uint64_t & rep)
                : helper_(helper),
                  rep_(rep),
                  k_len_(0) {}

    public:
        bool operator==(const sgt::Slice & k) const {
            return k == Key();
        }

        sgt::Slice Key() const {
            if (k_len_ == 0) {
                const_cast<KVTrans *>(this)->LoadKV();
            }
            return {s_.data(), k_len_};
        }

        bool Get(const sgt::Slice & k, std::string * v) const {
            if (reinterpret_cast<uintptr_t>(v) % 2 == 1) { // internal backdoor
                auto * p = reinterpret_cast<uint64_t *>(reinterpret_cast<char *>(v) - 1);
                auto pv = *p;
                if (pv == UINT64_MAX) { // GetInternal
                    *p = rep_;
                    return true;
                } else { // AddInternal
                    if (operator==(k)) {
                        if (pv > rep_) {
                            const_cast<KVTrans *>(this)->rep_ = pv;
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }

            if (operator==(k)) {
                v->assign(s_.data() + k_len_, s_.size() - k_len_);
                return true;
            } else {
                return false;
            }
        }

    private:
        void LoadKV();
    };

    class Helper : public sgt::SignatureTreeTpl<KVTrans>::Helper {
    private:
        IndexImpl * index_;
        std::string backup_;

        friend class KVTrans;

    public:
        explicit Helper(IndexImpl * index)
                : index_(index) {}

        ~Helper() override = default;

    public:
        uint64_t Add(const sgt::Slice & k, const sgt::Slice & v) override;

        void Del(KVTrans & trans) override;

        uint64_t Pack(size_t offset) const override {
            return NodeRep(offset);
        }

        size_t Unpack(const uint64_t & rep) const override {
            return GetNodeOffset(rep);
        }

        bool IsPacked(const uint64_t & rep) const override {
            return IsNode(rep);
        }

        KVTrans Trans(const uint64_t & rep) const override {
            return KVTrans(const_cast<Helper *>(this), const_cast<uint64_t &>(rep));
        }

        uint64_t GetNullRep() const override {
            return UINT64_MAX;
        }
    };

    class Allocator : public sgt::Allocator {
    private:
        std::unique_ptr<penv::MmapFile> file_;
        size_t alloc_;
        int64_t recycle_;

        friend class IndexImpl;

    public:
        explicit Allocator(std::unique_ptr<penv::MmapFile> && file)
                : file_(std::move(file)),
                  alloc_(0),
                  recycle_(-1) {
            file_->Hint(penv::MmapFile::RANDOM);
        }

        Allocator(std::unique_ptr<penv::MmapFile> && file,
                  size_t alloc, int64_t recycle)
                : file_(std::move(file)),
                  alloc_(alloc),
                  recycle_(recycle) {
            file_->Hint(penv::MmapFile::RANDOM);
        }

        ~Allocator() override = default;

    public:
        void * Base() override {
            return file_->Base();
        }

        const void * Base() const override {
            return file_->Base();
        }

        size_t AllocatePage() override {
            size_t offset;
            if (recycle_ >= 0) {
                offset = static_cast<size_t>(recycle_);
                recycle_ = *reinterpret_cast<int64_t *>(reinterpret_cast<char *>(Base()) + offset);
            } else {
                offset = alloc_;
                size_t occupy = offset + sgt::kPageSize;
                if (occupy > file_->GetFileSize()) {
                    throw sgt::AllocatorFullException();
                } else {
                    alloc_ = occupy;
                }
            }
            return offset;
        }

        void FreePage(size_t offset) override {
            *reinterpret_cast<int64_t *>(reinterpret_cast<char *>(Base()) + offset) = recycle_;
            recycle_ = static_cast<int64_t>(offset);
        }

        void Grow() override {
            file_->Resize(file_->GetFileSize() * 2);
        }
    };

    class IndexImpl : public Index {
    private:
        Helper helper_;
        Allocator allocator_;
        sgt::SignatureTreeTpl<KVTrans> tree_;

        StoreManager * manager_;
        size_t seq_;
        std::shared_ptr<Store> curr_;
        mutable std::mutex mutex_;

        friend class KVTrans;

        friend class Helper;

        friend class IteratorImpl;

    public:
        IndexImpl(std::unique_ptr<penv::MmapFile> && file, StoreManager * manager)
                : helper_(this),
                  allocator_(std::move(file)),
                  tree_(&helper_, &allocator_),
                  manager_(manager),
                  seq_(),
                  curr_(manager->OpenStoreForReadWrite(&seq_, nullptr)) {};

        IndexImpl(std::unique_ptr<penv::MmapFile> && file, StoreManager * manager,
                  size_t alloc, int64_t recycle)
                : helper_(this),
                  allocator_(std::move(file), alloc, recycle),
                  tree_(&helper_, &allocator_, 0),
                  manager_(manager),
                  seq_(),
                  curr_(manager->OpenStoreForReadWrite(&seq_, nullptr)) {};

        ~IndexImpl() override = default;

    public:
        bool Get(const Slice & k, std::string * v) const override {
            std::lock_guard guard(mutex_);
            return tree_.Get(k, v);
        }

        bool GetInternal(const Slice & k, uint64_t * v) const override {
            std::lock_guard guard(mutex_);
            *v = UINT64_MAX;
            return tree_.Get(k, reinterpret_cast<std::string *>(reinterpret_cast<char *>(v) + 1));
        }

        bool Add(const Slice & k, const Slice & v, bool overwrite) override {
            std::lock_guard guard(mutex_);
            restart:
            try {
                return tree_.Add(k, v, [&](KVTrans & trans, uint64_t & rep) -> bool {
                    if (!overwrite) {
                        return false;
                    } else {
                        rep = helper_.Add(k, v);
                        return true;
                    }
                });
            } catch (const StoreFullException &) {
                curr_ = manager_->OpenStoreForReadWrite(&seq_, curr_);
                goto restart;
            }
        }

        bool AddInternal(const Slice & k, uint64_t v) override {
            std::lock_guard guard(mutex_);
            return tree_.Get(k, reinterpret_cast<std::string *>(reinterpret_cast<char *>(v) + 1));
        }

        bool Del(const Slice & k) override {
            std::lock_guard guard(mutex_);
            restart:
            try {
                return tree_.Del(k);
            } catch (const StoreFullException &) {
                curr_ = manager_->OpenStoreForReadWrite(&seq_, curr_);
                goto restart;
            }
        }

        std::unique_ptr<Iterator>
        GetIterator() const override;

        void Sync() override {
            std::lock_guard guard(mutex_);
            curr_->Sync();
        }

        void RetireStore() override {
            std::lock_guard guard(mutex_);
            curr_ = manager_->OpenStoreForReadWrite(&seq_, curr_);
        }

        std::pair<size_t, int64_t>
        AllocatorInfo() const override {
            return {allocator_.alloc_, allocator_.recycle_};
        };
    };

    class IteratorImpl : public Iterator {
    private:
        IndexImpl * index_;
        sgt::SignatureTreeTpl<KVTrans>::IteratorImpl iter_;

    public:
        explicit IteratorImpl(IndexImpl * index)
                : index_(index),
                  iter_(index->tree_.GetIterator()) {}

        ~IteratorImpl() override = default;

    public:
        bool Valid() const override {
            return iter_.Valid();
        }

        void SeekToFirst() override {
            std::lock_guard guard(index_->mutex_);
            iter_.SeekToFirst();
        }

        void SeekToLast() override {
            std::lock_guard guard(index_->mutex_);
            iter_.SeekToLast();
        }

        void Seek(const Slice & target) override {
            std::lock_guard guard(index_->mutex_);
            iter_.Seek(target);
            if (Valid() && SliceComparator()(Key(), target)) {
                do {
                    Next();
                } while (Valid() && SliceComparator()(Key(), target));
            } else if (Valid() && SliceComparator()(target, Key())) {
                int i = 0;
                for (auto mirror = iter_;
                     mirror.Valid() && SliceComparator()(target, mirror.Key());
                     mirror.Prev(), ++i) {
                }
                for (--i; i > 0; --i) {
                    Prev();
                }
            }
        }

        void Next() override {
            iter_.Next();
        }

        void Prev() override {
            iter_.Prev();
        }

        Slice Key() const override {
            return iter_.Key();
        }

        Slice Value() const override {
            return iter_.Value();
        }
    };

    std::unique_ptr<Iterator>
    IndexImpl::GetIterator() const {
        return std::make_unique<IteratorImpl>(const_cast<IndexImpl *>(this));
    }

    void KVTrans::LoadKV() {
        auto[seq, id] = GetKVSeqAndID(rep_);
        auto store = helper_->index_->seq_ != seq ? helper_->index_->manager_->OpenStoreForRandomRead(seq)
                                                  : helper_->index_->curr_;
        helper_->backup_.clear();
        store->Get(id, &helper_->backup_);
        s_ = helper_->backup_;
        logream::GetVarint32(&s_, &k_len_);
    }

    uint64_t Helper::Add(const sgt::Slice & k, const sgt::Slice & v) {
        backup_.clear();
        logream::PutVarint32(&backup_, static_cast<uint32_t>(k.size()));
        backup_.append(k.data(), k.size());
        backup_.append(v.data(), v.size());
        auto id = index_->curr_->Add(backup_, false);
        return KVRep(static_cast<uint32_t>(index_->seq_), static_cast<uint32_t>(id));
    }

    void Helper::Del(levidb::KVTrans & trans) {
        backup_.clear();
        logream::PutVarint32(&backup_, 0);
        Slice k = trans.Key();
        backup_.append(k.data(), k.size());
        index_->curr_->Add(backup_, false);
    }

    std::unique_ptr<Index>
    Index::Open(const std::string & fname, StoreManager * manager) {
        return std::make_unique<IndexImpl>(penv::Env::Default()->OpenMmapFile(fname), manager);
    }

    std::unique_ptr<Index>
    Index::Reopen(const std::string & fname, StoreManager * manager,
                  size_t alloc, int64_t recycle) {
        return std::make_unique<IndexImpl>(penv::Env::Default()->ReopenMmapFile(fname), manager,
                                           alloc, recycle);
    }
}