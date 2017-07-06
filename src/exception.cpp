#include "exception.h"

namespace LeviDB {
    const char * Exception::_what = "LeviDBException";

    std::unique_ptr<char[]> Exception::copyState(const char * s) noexcept {
        uint32_t size;
        memcpy(&size, s, sizeof(size));

        auto res = std::unique_ptr<char[]>(new char[size + 5]);
        memcpy(res.get(), s, size + 5);
        return res;
    }

    Exception::Exception(Code code, const Slice & msg, const Slice & msg2) noexcept {
        assert(code != 0);

        const uint32_t len = static_cast<uint32_t>(msg.size());
        const uint32_t len2 = static_cast<uint32_t>(msg2.size());
        const uint32_t size = len + (len2 ? (2 + len2) : 0);

        auto res = std::unique_ptr<char[]>(new char[size + 5]);
        memcpy(res.get(), &size, sizeof(size));
        res[4] = static_cast<char>(code);
        memcpy(res.get() + 5, msg.data(), len);
        if (len2) {
            res[5 + len] = ':';
            res[6 + len] = ' ';
            memcpy(res.get() + 7 + len, msg2.data(), len2);
        }

        _state = std::move(res);
    }

    std::string Exception::toString() const noexcept {
        assert(_state != nullptr);

        char tmp[30];
        const char * type;
        switch (code()) {
            case NOT_FOUND:
                type = "NotFound: ";
                break;
            case CORRUPTION:
                type = "Corruption: ";
                break;
            case NOT_SUPPORTED:
                type = "Not implemented: ";
                break;
            case INVALID_ARGUMENT:
                type = "Invalid argument: ";
                break;
            case IO_ERROR:
                type = "IO error: ";
                break;
            default:
                snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
                type = tmp;
                break;
        }

        std::string res(type);
        uint32_t len;
        memcpy(&len, _state.get(), sizeof(len));
        res.append(_state.get() + 5, len);
        return res;
    }
}