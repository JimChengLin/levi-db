#ifndef LEVIDB_META_KEEPER_H
#define LEVIDB_META_KEEPER_H

/*
 * 保存 meta 到非易失存储器
 */

#include "config.h"
#include "crc32c.h"
#include "env_io.h"
#include "exception.h"

namespace levidb8 {
    template<typename T>
    class WeakKeeper {
    private:
        std::string _backing_filename;
        std::string _trailing;
        T _value{};
        static_assert(std::is_standard_layout<T>::value, "layout could change");

    public:
        explicit WeakKeeper(std::string fname) : _backing_filename(std::move(fname)) {
            {
                SequentialFile file(_backing_filename);
                file.read(sizeof(_value), reinterpret_cast<char *>(&_value));

                Slice output;
                char buf[kPageSize]{};
                do {
                    output = file.read(kPageSize, buf);
                    _trailing.insert(_trailing.end(), output.data(), output.data() + output.size());
                } while (output.size() == kPageSize);

                uint32_t checksum;
                if (_trailing.size() < sizeof(checksum)) {
                    throw Exception::corruptionException("checksum lost", _backing_filename);
                }
                memcpy(&checksum, _trailing.data() + _trailing.size() - sizeof(checksum), sizeof(checksum));
                _trailing.resize(_trailing.size() - sizeof(checksum));

                uint32_t calc_checksum = crc32c::value(reinterpret_cast<const char *>(&_value), sizeof(_value));
                calc_checksum = crc32c::extend(calc_checksum, _trailing.data(), _trailing.size());
                if (calc_checksum != checksum) {
                    throw Exception::corruptionException("checksum mismatch", _backing_filename);
                }
            }
            env_io::deleteFile(_backing_filename);
        }

        WeakKeeper(std::string fname, T value, std::string trailing) noexcept
                : _backing_filename(std::move(fname)), _trailing(std::move(trailing)), _value(value) {}

        ~WeakKeeper() noexcept {
            uint32_t checksum = crc32c::extend(crc32c::value(reinterpret_cast<const char *>(&_value), sizeof(_value)),
                                               _trailing.data(), _trailing.size());
            assert(!env_io::fileExists(_backing_filename));
            try {
                AppendableFile file(std::move(_backing_filename));
                file.append({reinterpret_cast<const char *>(&_value), sizeof(_value)});
                file.append(_trailing);
                file.append({reinterpret_cast<const char *>(&checksum), sizeof(checksum)});
                file.sync();
            } catch (const Exception &) {
            }
        }

        EXPOSE(_trailing);

        EXPOSE(_value);
    };
}

#endif //LEVIDB_META_KEEPER_H