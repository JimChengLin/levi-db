#include <cstring>
#include <nmmintrin.h>

#include "crc32c.h"

namespace levidb8 {
    namespace crc32c {
        static inline uint32_t load32(const uint8_t * ptr) noexcept {
            uint32_t result;
            memcpy(&result, ptr, sizeof(result));
            return result;
        }

        static inline uint64_t load64(const uint8_t * ptr) noexcept {
            uint64_t result;
            memcpy(&result, ptr, sizeof(result));
            return result;
        }

        uint32_t extend(uint32_t crc, const char * buf, size_t size) noexcept {
            const auto * p = reinterpret_cast<const uint8_t *>(buf);
            const uint8_t * e = p + size;
            uint32_t l = (crc ^ 0xffffffffu);

#define STEP1 do {                              \
    l = _mm_crc32_u8(l, *p++);                  \
} while (false)
#define STEP4 do {                              \
    l = _mm_crc32_u32(l, load32(p));            \
    p += 4;                                     \
} while (false)
#define STEP8 do {                              \
    l = _mm_crc32_u64(l, load64(p));            \
    p += 8;                                     \
} while (false)

            if (size > 16) {
                for (size_t i = reinterpret_cast<uintptr_t>(p) % 8; i != 0; --i) {
                    STEP1;
                }

#if defined(_M_X64) || defined(__x86_64__)
                while ((e - p) >= 8) {
                    STEP8;
                }
                if ((e - p) >= 4) {
                    STEP4;
                }
#else  // !(defined(_M_X64) || defined(__x86_64__))
                while ((e-p) >= 4) {
                  STEP4;
                }
#endif  // defined(_M_X64) || defined(__x86_64__)
            }

            while (p != e) {
                STEP1;
            }
            return l ^ 0xffffffffu;
        }
    }
}