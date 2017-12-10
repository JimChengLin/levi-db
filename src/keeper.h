#ifndef LEVIDB_META_KEEPER_H
#define LEVIDB_META_KEEPER_H

/*
 * 保存 meta 到非易失存储器
 */

#include "../include/exception.h"
#include "crc32c.h"
#include "env_io.h"

namespace levidb8 {
    template<typename T>
    class WeakKeeper {
    private:
        std::string _backing_filename;
        T _value{};
        static_assert(std::is_standard_layout<T>::value, "layout could change");

    public:
        explicit WeakKeeper(std::string fname) : _backing_filename(std::move(fname)) {
            {
                SequentialFile file(_backing_filename);
                file.read(sizeof(_value), reinterpret_cast<char *>(&_value));
                uint32_t checksum;
                file.read(sizeof(checksum), reinterpret_cast<char *>(&checksum));

                uint32_t calc_checksum = crc32c::value(reinterpret_cast<const char *>(&_value), sizeof(_value));
                if (calc_checksum != checksum) {
                    throw Exception::corruptionException("checksum mismatch", _backing_filename);
                }
            }
            env_io::deleteFile(_backing_filename);
        }

        WeakKeeper(std::string fname, T value) noexcept : _backing_filename(std::move(fname)), _value(value) {}

        ~WeakKeeper() noexcept {
            uint32_t checksum = crc32c::value(reinterpret_cast<const char *>(&_value), sizeof(_value));
            assert(!env_io::fileExist(_backing_filename));
            try {
                AppendableFile file(std::move(_backing_filename));
                file.append({reinterpret_cast<const char *>(&_value), sizeof(_value)});
                file.append({reinterpret_cast<const char *>(&checksum), sizeof(checksum)});
                file.sync();
            } catch (const Exception &) {}
        }

        EXPOSE(_value);
    };
}

#endif //LEVIDB_META_KEEPER_H