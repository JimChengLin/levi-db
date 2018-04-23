#pragma once
#ifndef LEVIDB_MANIFESTER_H
#define LEVIDB_MANIFESTER_H

/*
 * Metadata 存储接口
 */

#include "slice.h"

namespace levidb {
    class Manifestor {
    public:
        Manifestor() = default;

        virtual ~Manifestor() = default;

    public:
        virtual void Set(const Slice & k, const Slice & v) = 0;

        virtual bool Get(const Slice & k, std::string * v) const = 0;

        virtual void Set(const Slice & k, int64_t v) {
            Set(k, {reinterpret_cast<char *>(&v), sizeof(v)});
        }

        virtual bool Get(const Slice & k, int64_t * v) const {
            std::string buf;
            if (Get(k, &buf)) {
                assert(buf.size() == sizeof(*v));
                memcpy(v, buf.data(), buf.size());
                return true;
            }
            return false;
        }

        virtual void Set(const Slice & k, double v) {
            Set(k, {reinterpret_cast<char *>(&v), sizeof(v)});
        }

        virtual bool Get(const Slice & k, double * v) const {
            std::string buf;
            if (Get(k, &buf)) {
                assert(buf.size() == sizeof(*v));
                memcpy(v, buf.data(), buf.size());
                return true;
            }
            return false;
        }
    };
}

#endif //LEVIDB_MANIFESTER_H
