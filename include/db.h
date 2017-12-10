#pragma once
#ifndef LEVIDB8_DB_H
#define LEVIDB8_DB_H

/*
 * DB 接口
 */

#include <functional>

#include "exception.h"
#include "iterator.h"
#include "options.h"

namespace levidb8 {
    class DB {
    public:
        DB() noexcept = default;

    public:
        virtual ~DB() noexcept = default;

        virtual bool put(const Slice & key,
                         const Slice & value,
                         const PutOptions & options) = 0;

        virtual bool remove(const Slice & key,
                            const RemoveOptions & options) = 0;

        // kvs 必须升序排列
        // nullSlice 作为 value = del
        virtual bool write(const std::pair<Slice, Slice> * kvs, size_t n,
                           const WriteOptions & options) = 0;

        virtual std::pair<std::string, bool/* success */>
        get(const Slice & key) const = 0;

        virtual std::unique_ptr<Iterator<Slice/* K */, Slice/* V */, bool/* del */>>
        scan() const = 0;

        virtual bool exist(const Slice & key) const = 0;

        virtual void sync() = 0;

    public:
        static std::unique_ptr<DB>
        open(const std::string & name,
             const OpenOptions & options);

    };

    bool repairDB(const std::string & name,
                  std::function<void(const Exception &, uint32_t/* pos */)> reporter) noexcept;

    void destroyDB(const std::string & name);
}

#endif //LEVIDB8_DB_H
