#pragma once
#ifndef LEVIDB8_INDEX_READ_H
#define LEVIDB8_INDEX_READ_H

#include "index.h"

namespace levidb8 {
    using KVI = Iterator<Slice, Slice>;

    class BitDegradeTreeReadLog : public BitDegradeTree {
    private:
        RandomAccessFile * _data_file;

    public:
        BitDegradeTreeReadLog(const std::string & fname, RandomAccessFile * data_file);

        BitDegradeTreeReadLog(const std::string & fname, OffsetToEmpty empty, RandomAccessFile * data_file);

        EXPOSE(_empty);

        std::pair<std::string/* res */, bool/* success */>
        find(const Slice & k) const;

        std::unique_ptr<KVI>
        scan() const noexcept;

    protected:
        std::unique_ptr<Matcher> offToMatcher(OffsetToData data) const noexcept override;

        std::unique_ptr<Matcher> sliceToMatcher(const Slice & slice) const noexcept override;
    };
}

#endif //LEVIDB8_INDEX_READ_H
