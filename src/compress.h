#pragma once
#ifndef LEVIDB8_COMPRESS_H
#define LEVIDB8_COMPRESS_H

#include <memory>
#include <vector>
#include <zstd.h>

#include "../include/slice.h"
#include "config.h"
#include "simple_iterator.h"

namespace levidb8 {
    namespace compress {
        std::vector<uint8_t> encode(const Slice & src);

        class Decoder {
        private:
            std::unique_ptr<SimpleIterator<Slice>> _src;
            Slice _item;
            int _line = 0;
            char _buff_out[kLogBlockSize];

            struct DSD {
                void operator()(ZSTD_DStream * dstream) noexcept {
                    ZSTD_freeDStream(dstream);
                }
            };

            std::unique_ptr<ZSTD_DStream, DSD> _dstream;
            ZSTD_inBuffer _input{};
            bool _lastPage{};

        public:
            explicit Decoder(std::unique_ptr<SimpleIterator<Slice>> && src);

            bool valid() const noexcept;

            Slice item() const noexcept;

            bool isLastPage() const noexcept;

            void next();

            void reset();
        };
    }
}

#endif //LEVIDB8_COMPRESS_H
