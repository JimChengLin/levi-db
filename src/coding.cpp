#include "coding.h"

namespace LeviDB {
    namespace Coding {
        char * encodeVarint32(char * dst, uint32_t value) noexcept {
            auto * p = reinterpret_cast<uint8_t *>(dst);
            static constexpr int B = 128;
            if (value < (1 << 7)) {
                *(p++) = (uint8_t) (value);
            } else if (value < (1 << 14)) {
                *(p++) = (uint8_t) (value | B);
                *(p++) = (uint8_t) (value >> 7);
            } else if (value < (1 << 21)) {
                *(p++) = (uint8_t) (value | B);
                *(p++) = (uint8_t) ((value >> 7) | B);
                *(p++) = (uint8_t) (value >> 14);
            } else if (value < (1 << 28)) {
                *(p++) = (uint8_t) (value | B);
                *(p++) = (uint8_t) ((value >> 7) | B);
                *(p++) = (uint8_t) ((value >> 14) | B);
                *(p++) = (uint8_t) (value >> 21);
            } else {
                *(p++) = (uint8_t) (value | B);
                *(p++) = (uint8_t) ((value >> 7) | B);
                *(p++) = (uint8_t) ((value >> 14) | B);
                *(p++) = (uint8_t) ((value >> 21) | B);
                *(p++) = (uint8_t) (value >> 28);
            }
            return reinterpret_cast<char *>(p);
        };

        static const char * decodeVarint32Fallback(const char * p, const char * limit, uint32_t * value) noexcept {
            uint32_t res = 0;
            for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
                uint32_t byte = *(reinterpret_cast<const uint8_t *>(p));
                p++;
                if (byte & 128) {
                    res |= ((byte & 127) << shift);
                } else {
                    res |= (byte << shift);
                    *value = res;
                    return p;
                }
            }
            return nullptr;
        }

        const char * decodeVarint32(const char * p, const char * limit, uint32_t * value) noexcept {
            if (p < limit) {
                uint32_t res = *(reinterpret_cast<const uint8_t *>(p));
                if ((res & 128) == 0) {
                    *value = res;
                    return p + 1;
                }
            }
            return decodeVarint32Fallback(p, limit, value);
        }
    }
}
