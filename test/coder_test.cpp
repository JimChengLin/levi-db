#include "../src/coder.h"

void coder_test() {
    LeviDB::SubCoderNYT encoder;
    std::string src = "JimZuoLin";

    std::vector<uint8_t> output(1);
    int nth_bit = CHAR_BIT - 1;
    for (char c:src) {
        encoder.encode(c, output, nth_bit);
    }
    encoder.finishEncode(output, nth_bit);

    LeviDB::SubCoderNYT decoder;
    LeviDB::SubCoderNormal * decoder_ = reinterpret_cast<LeviDB::SubCoderNormal *>(&decoder);
    LeviDB::Slice slice(reinterpret_cast<const char *>(output.data()), output.size());
}