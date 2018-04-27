#pragma once
#ifndef LEVIDB_ITERATOR_MERGER_H
#define LEVIDB_ITERATOR_MERGER_H

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>
#include <vector>

#include "../include/iterator.h"

namespace levidb {
    class IteratorMerger : public Iterator {
    private:
        std::vector<std::unique_ptr<Iterator>> iters_;
        Iterator * cursor_;

        enum Direction {
            kForward,
            kReverse
        };
        Direction direction_;

    public:
        explicit IteratorMerger(std::vector<std::unique_ptr<Iterator>> && iters)
                : iters_(std::move(iters)),
                  cursor_(nullptr),
                  direction_(kForward) {}

        ~IteratorMerger() override = default;

    public:
        bool Valid() const override;

        void SeekToFirst() override;

        void SeekToLast() override;

        void Seek(const Slice & target) override;

        void Next() override;

        void Prev() override;

        Slice Key() const override;

        Slice Value() const override;

    private:
        void FindSmallest();

        void FindLargest();
    };
}

#endif //LEVIDB_ITERATOR_MERGER_H
