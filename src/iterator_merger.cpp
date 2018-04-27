#include "iterator_merger.h"

namespace levidb {
    bool IteratorMerger::Valid() const {
        return cursor_ != nullptr;
    }

    void IteratorMerger::SeekToFirst() {
        for (auto & iter:iters_) {
            iter->SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void IteratorMerger::SeekToLast() {
        for (auto & iter:iters_) {
            iter->SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void IteratorMerger::Seek(const Slice & target) {
        for (auto & iter:iters_) {
            iter->Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

    void IteratorMerger::Next() {
        assert(Valid());

        if (direction_ != kForward) {
            Slice k = Key();
            for (auto & iter:iters_) {
                if (iter.get() != cursor_) {
                    iter->Seek(k);
                }
                if (iter->Valid() && k == iter->Key()) {
                    iter->Next();
                }
            }
            direction_ = kForward;
        }

        cursor_->Next();
        FindSmallest();
    }

    void IteratorMerger::Prev() {
        assert(Valid());

        if (direction_ != kReverse) {
            Slice k = Key();
            for (auto & iter:iters_) {
                if (iter.get() != cursor_) {
                    iter->Seek(k);
                }
                if (iter->Valid()) {
                    iter->Prev();
                } else {
                    iter->SeekToLast();
                }
            }
            direction_ = kReverse;
        }

        cursor_->Prev();
        FindLargest();
    }

    Slice IteratorMerger::Key() const {
        assert(Valid());
        return cursor_->Key();
    }

    Slice IteratorMerger::Value() const {
        assert(Valid());
        return cursor_->Value();
    }

    void IteratorMerger::FindSmallest() {
        Iterator * smallest = nullptr;
        for (auto & iter:iters_) {
            if (iter->Valid()) {
                if (smallest == nullptr) {
                    smallest = iter.get();
                } else if (SliceComparator()(iter->Key(), smallest->Key())) {
                    smallest = iter.get();
                }
            }
        }
        cursor_ = smallest;
    }

    void IteratorMerger::FindLargest() {
        Iterator * largest = nullptr;
        for (auto & iter:iters_) {
            if (iter->Valid()) {
                if (largest == nullptr) {
                    largest = iter.get();
                } else if (SliceComparator()(largest->Key(), iter->Key())) {
                    largest = iter.get();
                }
            }
        }
        cursor_ = largest;
    }
}