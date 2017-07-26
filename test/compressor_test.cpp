#include "../src/coder.h"
#include "../src/compressor.h"
#include <iostream>

void compressor_test() noexcept {
    const std::string sample("123456789");
    static constexpr int sample_len = 9;

    LeviDB::Compressor compressor;
    uint32_t cursor = 0;

    auto req = [&compressor, &cursor](int length) {
        // don't know precise prefix length(varint32)
        auto res = compressor.nextCompressed(32768/* max(2^15) */, cursor);
        cursor += length;
        return res;
    };

    compressor.submit(sample); // no compression
    auto res_0 = req(sample_len);
    assert(res_0.first.size() == 14 && res_0.second == LeviDB::CompressorConst::NO_COMPRESS);

    compressor.submit(sample/* U8U8U8 */);
    auto res_1 = req(sample_len);
    assert(res_1.first.size() == 8 && res_1.second == LeviDB::CompressorConst::SIMPLE_COMPRESS);

    compressor.submit(std::string(UINT8_MAX + 10, 'A')/* U8(0)U8U16 */+ std::string(10, 'B')/* U8(0)U16U8 */);
    auto res_2 = req(UINT8_MAX + 10 + 10);
    assert(res_2.first.size() == 18);

    compressor.submit(std::string(UINT8_MAX + 10, 'C') + std::string(UINT8_MAX + 10, 'D')/* U8(0)U16U16 */);
    auto res_3 = req((UINT8_MAX + 10) * 2);
    assert(res_3.first.size() == 19);

    compressor.submit(sample/* U16U8U8 */);
    auto res_4 = req(sample_len);
    assert(res_4.first.size() == 10);

    compressor.submit(std::string(UINT8_MAX + 10, 'A')/* U16U8U16 */);
    auto res_5 = req(UINT8_MAX + 10);
    assert(res_5.first.size() == 12);

    compressor.submit(std::string(10, 'B')/* U16U16U8 */);
    auto res_6 = req(10);
    assert(res_6.first.size() == 12);

    compressor.submit(std::string(UINT8_MAX + 10, 'D')/* U16U16U16 */);
    auto res_7 = req(10);
    assert(res_7.first.size() == 14);

    // spec cmd
    compressor.submit(sample);
    compressor.emitSpecCmd(0, 1/* physical "from" */, sample_len);
    auto res_8 = req(sample_len);
    assert(res_8.first.size() == 11);

    std::cout << __FUNCTION__ << std::endl;
}