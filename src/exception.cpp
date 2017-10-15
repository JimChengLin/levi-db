#include "exception.h"

namespace LeviDB {
    // header: uint32_t len + uint8_t code
    std::unique_ptr<char[]> Exception::copyState(const char * s) noexcept {
        uint32_t size;
        memcpy(&size, s, sizeof(size));

        auto res = std::unique_ptr<char[]>(new char[size + 5]);
        memcpy(res.get(), s, size + 5);
        return res;
    }

    Exception::Exception(Code code, const Slice & msg, const Slice & msg2) noexcept {
        assert(code != 0);

        const size_t len = msg.size();
        const size_t len2 = msg2.size();
        const size_t size = len + (len2 != 0 ? (2 + len2) : 0);

        auto res = std::unique_ptr<char[]>(new char[size + 5]);
        memcpy(res.get(), &size, sizeof(size));
        res[4] = static_cast<char>(code);

        memcpy(res.get() + 5, msg.data(), len);
        if (len2 != 0) {
            res[5 + len] = ':';
            res[6 + len] = ' ';
            memcpy(res.get() + 7 + len, msg2.data(), len2);
        }

        _state = std::move(res);
    }

    std::string Exception::toString() const noexcept {
        assert(_state != nullptr);

        std::string res;
        switch (code()) {
            case NOT_FOUND:
                res = "NotFound: ";
                break;
            case CORRUPTION:
                res = "Corruption: ";
                break;
            case NOT_SUPPORTED:
                res = "Not implemented: ";
                break;
            case INVALID_ARGUMENT:
                res = "Invalid argument: ";
                break;
            case IO_ERROR:
                res = "IO error: ";
                break;
            default:
                assert(false);
        }

        uint32_t len;
        memcpy(&len, _state.get(), sizeof(len));
        res.append(_state.get() + 5, len);
        return res;
    }
}