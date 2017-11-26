#include <iostream>
#include <random>

#include "../src/compress.h"

class CompressDataIter : public levidb8::SimpleIterator<levidb8::Slice> {
private:
    std::vector<uint8_t> _compress_data;
    std::vector<uint8_t>::const_iterator _cursor;

public:
    explicit CompressDataIter(std::vector<uint8_t> compress_data) noexcept
            : _compress_data(std::move(compress_data)), _cursor(_compress_data.cend()) {}

    ~CompressDataIter() noexcept override = default;

    bool valid() const override {
        return _cursor != _compress_data.cend();
    }

    void prepare() override {
        _cursor = _compress_data.cbegin();
    };

    void next() override {
        _cursor += std::min<size_t>(1024, _compress_data.size() - (_cursor - _compress_data.cbegin()));
    }

    levidb8::Slice item() const override {
        return {_cursor.base(),
                std::min<size_t>(1024, _compress_data.size() - (_cursor - _compress_data.cbegin()))};
    }
};

void compress_test() {
    static constexpr int test_times = 10000;
    std::default_random_engine gen{std::random_device{}()};
    std::vector<uint8_t> src;
    src.reserve(test_times);
    for (int i = 0; i < src.capacity(); ++i) {
        src.emplace_back(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(gen));
    }

    {
        std::vector<uint8_t> compress_data = levidb8::compressor::encode(levidb8::Slice(src.data(), src.size()));
        auto uncompress_iter = levidb8::compressor::makeDecodeIterator(
                std::make_unique<CompressDataIter>(compress_data));
        std::vector<uint8_t> uncompress_data;
        uncompress_iter->prepare();
        while (true) {
            uncompress_iter->next();
            if (!uncompress_iter->valid()) {
                break;
            }
            uncompress_data.insert(uncompress_data.end(),
                                   reinterpret_cast<const uint8_t *>(uncompress_iter->item().data()),
                                   reinterpret_cast<const uint8_t *>(
                                           uncompress_iter->item().data() + uncompress_iter->item().size()));
        }
        assert(src == uncompress_data);
    }

    std::cout << __FUNCTION__ << std::endl;
};