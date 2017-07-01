#include "../src/coder.h"
#include <iostream>

void coder_test() {
    std::string src = "JimZuoLin";

    LeviDB::SubCoderNYT encoder;
    std::vector<uint8_t> encode_res(1);
    int nth_bit = CHAR_BIT - 1;
    for (char c:src) {
        encoder.encode(c, encode_res, nth_bit);
    }
    encoder.finishEncode(encode_res, nth_bit);

    LeviDB::SubCoderNYT decoder;
    LeviDB::SubCoderNormal * forward_decoder = reinterpret_cast<LeviDB::SubCoderNormal *>(&decoder);
    LeviDB::Slice slice(reinterpret_cast<const char *>(encode_res.data()), encode_res.size());
    size_t nth_byte_;
    forward_decoder->initDecode(slice, nth_byte_, nth_bit);

    std::string decode_res;
    char decode_char;
    while ((decode_char = static_cast<char>(forward_decoder->decode(slice, nth_byte_, nth_bit))) !=
           LeviDB::CoderConst::decode_exit) {
        decode_res += decode_char;
    }
    assert(decode_res == src);
    decode_res.clear();

    LeviDB::SubCoderNYT backward_decoder;
    std::reverse(encode_res.begin(), encode_res.end());
    backward_decoder.initDecode(slice, nth_byte_, nth_bit);

    while ((decode_char = static_cast<char>(backward_decoder.decode(slice, nth_byte_, nth_bit))) !=
           LeviDB::CoderConst::decode_exit) {
        decode_res += decode_char;
    }
    assert(decode_res == src);
    // ---

    LeviDB::Holder holder;
    for (int i = 0; i < 10; ++i) {
        holder.plus(i + 1, 2);
    }
    for (int i = 0; i < 10; ++i) {
        assert(holder.getCum(i + 1) == (i + 1) * 2);
    }
    assert(holder.total == 20);
    holder.halve();
    for (int i = 0; i < 10; ++i) {
        assert(holder.getCum(i + 1) == i + 1);
    }
    assert(holder.total == 10);
    // ---

    std::cout << __FUNCTION__ << std::endl;
}