#include <iostream>
#include <random>

#include "../include/exception.h"
#include "../src/compress.h"

class CompressedDataIterator : public levidb8::SimpleIterator<levidb8::Slice> {
private:
    std::vector<uint8_t> _compress_data;
    std::vector<uint8_t>::const_iterator _cursor;

public:
    explicit CompressedDataIterator(std::vector<uint8_t> compress_data) noexcept
            : _compress_data(std::move(compress_data)), _cursor(_compress_data.cend()) {}

    ~CompressedDataIterator() noexcept override = default;

    bool valid() const override {
        return _cursor != _compress_data.cend();
    }

    void next() override {
        if (_cursor == _compress_data.cend()) {
            _cursor = _compress_data.cbegin();
        } else {
            _cursor += std::min<size_t>(1024, _compress_data.cend() - _cursor);
        }
    }

    levidb8::Slice item() const override {
        return {_cursor.base(), std::min<size_t>(1024, _compress_data.cend() - _cursor)};
    }
};

void compress_test() {
    static constexpr int test_times = 10000;
    std::default_random_engine gen{std::random_device{}()};

    std::vector<uint8_t> src(test_times);
    for (uint8_t & val:src) {
        val = std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(gen);
    }

    {
        std::vector<uint8_t> compressed_data = levidb8::compress::encode(src);
        std::vector<uint8_t> uncompress_data;
        levidb8::compress::Decoder uncompress_iter(std::make_unique<CompressedDataIterator>(compressed_data));
        while (true) {
            uncompress_iter.next();
            if (!uncompress_iter.valid()) {
                break;
            }
            uncompress_data.insert(uncompress_data.end(),
                                   reinterpret_cast<const uint8_t *>(uncompress_iter.item().data()),
                                   reinterpret_cast<const uint8_t *>(
                                           uncompress_iter.item().data() + uncompress_iter.item().size()));
        }
        assert(uncompress_data == src);
    }

    std::cout << __FUNCTION__ << std::endl;
};