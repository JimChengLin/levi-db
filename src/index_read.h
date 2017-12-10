#pragma once
#ifndef LEVIDB8_INDEX_READ_H
#define LEVIDB8_INDEX_READ_H

#include "index.h"
#include "index.hpp"
#include "index_scan.hpp"
#include "log_reader.h"

namespace levidb8 {
    class MatcherOffsetImpl;

    class MatcherSliceImpl;

    class RandomAccessFile;

    struct CacheImpl {
        RandomAccessFile * data_file;
        RecordCache record_cache;
    };

    class MatcherOffsetImpl {
    private:
        std::unique_ptr<RecordIterator> _iter;

    public:
        MatcherOffsetImpl(OffsetToData data, CacheImpl & cache);

        bool operator==(const Slice & another) const;

        Slice toSlice(const USR & usr) const;

        bool isCompress() const;
    };

    class MatcherSliceImpl {
    private:
        Slice _slice;

    public:
        explicit MatcherSliceImpl(Slice slice) noexcept;

        char operator[](size_t idx) const noexcept;

        size_t size() const noexcept;
    };

    class BitDegradeTreeRead : public BitDegradeTree<MatcherOffsetImpl, MatcherSliceImpl, CacheImpl> {
    public:
        BitDegradeTreeRead(const std::string & fname, RandomAccessFile * data_file);

        BitDegradeTreeRead(const std::string & fname, OffsetToEmpty empty, RandomAccessFile * data_file);

        EXPOSE(_empty);

        bool find(const Slice & k, std::string * value) const;

        std::unique_ptr<Iterator<Slice/* K */, Slice/* V */, bool/* del */>>
        scan() const noexcept;
    };
}

#endif //LEVIDB8_INDEX_READ_H
