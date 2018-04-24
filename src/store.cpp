#include "env.h"
#include "logream_lite.h"

#include "filename.h"
#include "store.h"

namespace levidb {
    class StoreSequentialReadImpl : public Store {
    private:
        class ReaderHelper : public logream::Reader::Helper {
        public:
            void ReadAt(size_t offset, size_t n, char * scratch) const override {

            }
        };

        logream::ReaderLite reader_;

    public:

    };

    std::unique_ptr<Store>
    Store::OpenForSequentialRead(const std::string & fname) {
        if (IsCompressedStore(fname)) {
            return OpenForRandomRead(fname);
        }
    }

    class CompressedStoreRandomReadImpl : public Store {

    };

    class PlainStoreRandomReadImpl : public Store {

    };

    std::unique_ptr<Store>
    Store::OpenForRandomRead(const std::string & fname) {
        if (IsCompressedStore(fname)) {

        } else {

        }
    }

    std::unique_ptr<Store>
    Store::OpenForReadWrite(const std::string & fname) {

    }

    std::unique_ptr<Store>
    Store::OpenForCompressedWrite(const std::string & fname) {

    }
}