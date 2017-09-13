#ifndef LEVIDB_META_KEEPER_H
#define LEVIDB_META_KEEPER_H

/*
 * 保存 meta 到非易失存储器
 *
 * keeper 分两类
 * 1. strong keeper
 * 采用 double-write buffer, update 成功后的任意时间点 crash, 再次打开依然能恢复数据
 * 2. weak keeper
 * 数据只有在析构时写入, crash 后不要求恢复数据, 但必须能报告错误以启动恢复组件
 *
 * 格式: T + trailing + checksum
 */

#include "env_io.h"
#include "exception.h"
#include "crc32c.h"

namespace LeviDB {
    template<typename T>
    class StrongKeeper {
    private:
        std::string _backing_filename;
        std::string _trailing;
        T _value{};
        static_assert(std::is_standard_layout<T>::value, "layout could change");

    public:
        typedef T value_type;

        explicit StrongKeeper(std::string fname) : _backing_filename(std::move(fname)) {
            for (const std::string & plan:{_backing_filename + "_a.keeper", _backing_filename + "_b.keeper"}) {
                if (!IOEnv::fileExists(plan)) {
                    continue;
                }

                SequentialFile file(plan);
                file.read(sizeof(_value), reinterpret_cast<char *>(&_value));

                Slice output;
                char buf[IOEnv::page_size_];
                do {
                    output = file.read(IOEnv::page_size_, buf);
                    _trailing.insert(_trailing.end(), output.data(), output.data() + output.size());
                } while (output.size() == IOEnv::page_size_);

                uint32_t checksum;
                if (_trailing.size() < sizeof(checksum)) {
                    continue;
                }
                memcpy(&checksum, _trailing.data() + _trailing.size() - sizeof(checksum), sizeof(checksum));
                _trailing.resize(_trailing.size() - sizeof(checksum));

                uint32_t exist_checksum = CRC32C::value(reinterpret_cast<const char *>(&_value), sizeof(_value));
                if (!_trailing.empty()) {
                    exist_checksum = CRC32C::extend(exist_checksum, _trailing.data(), _trailing.size());
                }
                if (exist_checksum != checksum) {
                    continue;
                }
                return;
            }
            throw Exception::notFoundException("no legal meta file", _backing_filename);
        }

        StrongKeeper(std::string fname, T value, std::string trailing)
                : _backing_filename(std::move(fname)), _trailing(std::move(trailing)), _value(value) {
            uint32_t checksum = CRC32C::extend(CRC32C::value(reinterpret_cast<const char *>(&_value), sizeof(_value)),
                                               _trailing.data(), _trailing.size());
            for (const std::string & plan:{_backing_filename + "_a.keeper", _backing_filename + "_b.keeper"}) {
                AppendableFile file(plan);
                file.append({reinterpret_cast<const char *>(&_value), sizeof(_value)});
                file.append(_trailing);
                file.append({reinterpret_cast<const char *>(&checksum), sizeof(checksum)});
                file.sync();
            }
        }

        template<typename FIELD>
        void update(size_t field_offset, FIELD field) {
            memcpy(reinterpret_cast<char *>(&_value) + field_offset, &field, sizeof(field));
            uint32_t checksum = CRC32C::extend(CRC32C::value(reinterpret_cast<const char *>(&_value), sizeof(_value)),
                                               _trailing.data(), _trailing.size());
            for (const std::string & plan:{_backing_filename + "_a.keeper", _backing_filename + "_b.keeper"}) {
                IOEnv::renameFile(plan, "temp.keeper");
                {
                    RandomWriteFile file("temp.keeper");
                    file.write(field_offset, {reinterpret_cast<const char *>(&field), sizeof(field)});
                    file.sync();
                }
                IOEnv::renameFile("temp.keeper", plan);
            }
        }

        void setTrailing(const Slice & data) {
            uint32_t checksum = CRC32C::extend(CRC32C::value(reinterpret_cast<const char *>(&_value), sizeof(_value)),
                                               data.data(), data.size());
            for (const std::string & plan:{_backing_filename + "_a.keeper", _backing_filename + "_b.keeper"}) {
                IOEnv::truncateFile(plan, sizeof(_value) + data.size() + sizeof(checksum));
                IOEnv::renameFile(plan, "temp.keeper");
                {
                    RandomWriteFile file("temp.keeper");
                    file.write(sizeof(_value), data);
                    file.write(sizeof(_value) + data.size(),
                               {reinterpret_cast<const char *>(&checksum), sizeof(checksum)});
                    file.sync();
                }
                IOEnv::renameFile("temp.keeper", plan);
            }
        }

        const T & immut_value() const noexcept {
            return _value;
        }

        const std::string & immut_trailing() const noexcept {
            return _trailing;
        }
    };

    template<typename T>
    class WeakKeeper {
        T _value;
    };
}

#endif //LEVIDB_META_KEEPER_H