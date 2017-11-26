#pragma once
#ifndef LEVIDB8_COMPRESS_H
#define LEVIDB8_COMPRESS_H

#include <memory>
#include <vector>

#include "iterator.h"
#include "slice.h"

namespace levidb8 {
    namespace compressor {
        std::vector<uint8_t> encode(const Slice & src);

        std::unique_ptr<SimpleIterator<Slice>>
        makeDecodeIterator(std::unique_ptr<SimpleIterator<Slice>> && src_iter) noexcept;
    }
}

#endif //LEVIDB8_COMPRESS_H
