#include "env.h"
#include "logream_lite.h"

#include "filename.h"
#include "store.h"

namespace levidb {
    class StoreSequentialReadImpl : public Store {
    private:
        class ReaderHelper : public logream::Reader::Helper {
        private:
            std::unique_ptr<penv::SequentialFile> file_;

        public:
            explicit ReaderHelper(std::unique_ptr<penv::SequentialFile> && file)
                    : file_(std::move(file)) {}

            void ReadAt(size_t offset, size_t n, char * scratch) const override {
                file_->Read(n, scratch);
            }
        };

        ReaderHelper reader_helper_;
        logream::ReaderLite reader_;

    public:
        explicit StoreSequentialReadImpl(std::unique_ptr<penv::SequentialFile> && file)
                : reader_helper_(std::move(file)),
                  reader_(&reader_helper_) {}

        ~StoreSequentialReadImpl() override = default;

    public:
        size_t Add(const Slice & s, bool sync) override {
            assert(false);
            return 0;
        }

        size_t Get(size_t id, std::string * s) const override {
            return reader_.Get(id, s);
        }
    };

    std::unique_ptr<Store>
    Store::OpenForSequentialRead(const std::string & fname) {
        if (IsCompressedStore(fname)) {
            return OpenForRandomRead(fname);
        }
        auto file = penv::Env::Default()->OpenSequentialFile(fname);
        return std::make_unique<StoreSequentialReadImpl>(std::move(file));
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