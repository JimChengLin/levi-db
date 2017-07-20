#include "../src/crc32c.h"
#include <cstring>
#include <cassert>
#include <iostream>

void crc32c_test() noexcept {
    char buf[32];

    memset(buf, 0, sizeof(buf));
    assert(LeviDB::CRC32C::value(buf, sizeof(buf)) == 0x8a9136aa);

    memset(buf, 0xff, sizeof(buf));
    assert(LeviDB::CRC32C::value(buf, sizeof(buf)) == 0x62a8ab43);

    for (char i = 0; i < 32; i++) {
        buf[i] = i;
    }
    assert(LeviDB::CRC32C::value(buf, sizeof(buf)) == 0x46dd794e);

    std::cout << __FUNCTION__ << std::endl;
}