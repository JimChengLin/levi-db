#include <iostream>
#include <random>

#include "../src/compress.h"

class CompressDataIter : public LeviDB::SimpleIterator<LeviDB::Slice> {
private:
    std::vector<uint8_t> _compress_data;
    std::vector<uint8_t>::const_iterator _cursor;

public:
    explicit CompressDataIter(std::vector<uint8_t> compress_data) noexcept
            : _compress_data(std::move(compress_data)), _cursor(_compress_data.cbegin()) {}

    DEFAULT_MOVE(CompressDataIter);

    CompressDataIter(const CompressDataIter &) = default;

    CompressDataIter & operator=(const CompressDataIter &) = default;

    ~CompressDataIter() noexcept override = default;

    bool valid() const override {
        return _cursor != _compress_data.cend();
    }

    void next() override {
        _cursor += std::min<size_t>(1024, _compress_data.size() - (_cursor - _compress_data.cbegin()));
    }

    LeviDB::Slice item() const override {
        return {_cursor.base(),
                std::min<size_t>(1024, _compress_data.size() - (_cursor - _compress_data.cbegin()))};
    }
};

void compress_test() {
    std::default_random_engine gen{std::random_device{}()};
    std::vector<uint8_t> src;
    src.reserve(65535);
    for (int i = 0; i < src.capacity(); ++i) {
        src.emplace_back(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(gen));
    }

    std::vector<uint8_t> compress_data = LeviDB::Compressor::encode(LeviDB::Slice(src.data(), src.size()));
    auto uncompress_iter = LeviDB::Compressor::makeDecodeIterator(std::make_unique<CompressDataIter>(compress_data));

    std::vector<uint8_t> uncompress_data;
    while (uncompress_iter->valid()) {
        uncompress_data.insert(uncompress_data.end(),
                               reinterpret_cast<const uint8_t *>(uncompress_iter->item().data()),
                               reinterpret_cast<const uint8_t *>(
                                       uncompress_iter->item().data() + uncompress_iter->item().size()));
        uncompress_iter->next();
    }
    assert(src == uncompress_data);

    std::cout << __FUNCTION__ << std::endl;
};