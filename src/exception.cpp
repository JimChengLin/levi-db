#include "../include/exception.h"

namespace levidb8 {
    std::string Exception::toString() const noexcept {
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
        }

        uint32_t len;
        memcpy(&len, _state.get(), sizeof(len));
        res.append(_state.get() + 5, len);
        return res;
    }

    Exception::Exception(Code code, const Slice & msg, const Slice & msg2) noexcept {
        const size_t len = msg.size();
        const size_t len2 = msg2.size();
        const auto size = static_cast<uint32_t>(len + len2 + (len2 != 0) * 2);

        _state = std::unique_ptr<char[]>(new char[size + 5]);
        memcpy(_state.get(), &size, sizeof(size));
        _state[4] = static_cast<char>(code);

        memcpy(_state.get() + 5, msg.data(), len);
        if (len2 != 0) {
            _state[5 + len] = ':';
            _state[6 + len] = ' ';
            memcpy(_state.get() + 7 + len, msg2.data(), len2);
        }
    }
}