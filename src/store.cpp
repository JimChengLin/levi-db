#include "defs.h"
#include "env.h"
#include "logream_compress.h"
#include "logream_lite.h"

#include "filename.h"
#include "store.h"

namespace levidb {
    class SequentialReaderHelper : public logream::Reader::Helper {
    private:
        std::unique_ptr<penv::SequentialFile> file_;

    public:
        explicit SequentialReaderHelper(std::unique_ptr<penv::SequentialFile> && file)
                : file_(std::move(file)) {}

        ~SequentialReaderHelper() override = default;

    public:
        void ReadAt(size_t offset, size_t n, char * scratch) const override {
            file_->Read(n, scratch);
        }
    };

    class RandomReaderHelper : public logream::Reader::Helper {
    private:
        std::unique_ptr<penv::RandomAccessFile> file_;

    public:
        explicit RandomReaderHelper(std::unique_ptr<penv::RandomAccessFile> && file)
                : file_(std::move(file)) {
            file_->Hint(penv::RandomAccessFile::RANDOM);
        }

        ~RandomReaderHelper() override = default;

    public:
        void ReadAt(size_t offset, size_t n, char * scratch) const override {
            file_->ReadAt(offset, n, scratch);
        }
    };

    class SequentialStore : public Store {
    private:
        SequentialReaderHelper reader_helper_;
        logream::ReaderLite reader_;

    public:
        explicit SequentialStore(std::unique_ptr<penv::SequentialFile> && file)
                : reader_helper_(std::move(file)),
                  reader_(&reader_helper_) {}

        ~SequentialStore() override = default;

    public:
        size_t Add(const Slice & s, bool sync) override {
            assert(false);
            return 0;
        }

        size_t Get(size_t id, std::string * s) const override {
            return reader_.Get(id, s);
        }
    };

    class CompressedRandomStore : public Store {
    private:
        RandomReaderHelper reader_helper_;
        logream::ReaderCompress reader_;

    public:
        explicit CompressedRandomStore(std::unique_ptr<penv::RandomAccessFile> && file)
                : reader_helper_(std::move(file)),
                  reader_(&reader_helper_) {}

        ~CompressedRandomStore() override = default;

    public:
        size_t Add(const Slice & s, bool sync) override {
            assert(false);
            return 0;
        }

        size_t Get(size_t id, std::string * s) const override {
            return reader_.Get(id, s);
        }
    };

    class PlainRandomStore : public Store {
    private:
        RandomReaderHelper reader_helper_;
        logream::ReaderLite reader_;

    public:
        explicit PlainRandomStore(std::unique_ptr<penv::RandomAccessFile> && file)
                : reader_helper_(std::move(file)),
                  reader_(&reader_helper_) {}

        ~PlainRandomStore() override = default;

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
            auto file = penv::Env::Default()->OpenRandomAccessFie(fname);
            file->Prefetch(0, penv::Env::Default()->GetFileSize(fname));
            return std::make_unique<CompressedRandomStore>(std::move(file));
        } else {
            auto file = penv::Env::Default()->OpenSequentialFile(fname);
            return std::make_unique<SequentialStore>(std::move(file));
        }
    }

    std::unique_ptr<Store>
    Store::OpenForRandomRead(const std::string & fname) {
        auto file = penv::Env::Default()->OpenRandomAccessFie(fname);
        if (IsCompressedStore(fname)) {
            return std::make_unique<CompressedRandomStore>(std::move(file));
        } else {
            return std::make_unique<PlainRandomStore>(std::move(file));
        }
    }

    class WriterHelper : public logream::Writer::Helper {
    private:
        std::unique_ptr<penv::WritableFile> file_;

        friend class ReadWriteStore;

    public:
        explicit WriterHelper(std::unique_ptr<penv::WritableFile> && file)
                : file_(std::move(file)) {}

        ~WriterHelper() override = default;

    public:
        void Write(const logream::Slice & s) override {
            size_t file_size = file_->GetFileSize();
            if (file_size + s.size() >= Store::kMaxSize) {
                throw StoreFullException();
            }
            file_->PrepareWrite(file_size, s.size());
            file_->Write(s);
        }
    };

    class BufferedWriterHelper : public logream::Writer::Helper {
    private:
        enum {
            kBufLimit = 4 * 1024 * 1024
        };

        std::unique_ptr<penv::WritableFile> file_;
        std::string buf_;

    public:
        explicit BufferedWriterHelper(std::unique_ptr<penv::WritableFile> && file)
                : file_(std::move(file)) {}

        ~BufferedWriterHelper() override {
            if (!buf_.empty()) {
                file_->Write(buf_);
            }
        };

    public:
        void Write(const logream::Slice & s) override {
            size_t file_size = file_->GetFileSize();
            if (file_size + buf_.size() + s.size() >= Store::kMaxSize) {
                throw StoreFullException();
            }
            if (buf_.size() >= kBufLimit) {
                file_->PrepareWrite(file_size, buf_.size());
                file_->Write(buf_);
                buf_.clear();
            } else {
                buf_.append(s.data(), s.size());
            }
        }
    };

    class ReadWriteStore : public Store {
    private:
        RandomReaderHelper reader_helper_;
        logream::ReaderLite reader_;
        WriterHelper writer_helper_;
        logream::WriterLite writer_;

    public:
        ReadWriteStore(std::unique_ptr<penv::RandomAccessFile> && r_file,
                       std::unique_ptr<penv::WritableFile> && w_file)
                : reader_helper_(std::move(r_file)),
                  reader_(&reader_helper_),
                  writer_helper_(std::move(w_file)),
                  writer_(&writer_helper_, 0) {}

        ~ReadWriteStore() override = default;

    public:
        size_t Add(const Slice & s, bool sync) override {
            size_t n = s.size();
            size_t pos = writer_.Add(s.data(), &n);
            if (sync) {
#if defined(PENV_OS_LINUX)
                writer_helper_.file_->RangeSync(pos,n);
#else
                writer_helper_.file_->Sync();
#endif
            }
            return pos;
        }

        size_t Get(size_t id, std::string * s) const override {
            return reader_.Get(id, s);
        }
    };

    std::unique_ptr<Store>
    Store::OpenForReadWrite(const std::string & fname) {
        auto r_file = penv::Env::Default()->OpenRandomAccessFie(fname);
        auto w_file = penv::Env::Default()->OpenWritableFile(fname);
        return std::make_unique<ReadWriteStore>(std::move(r_file), std::move(w_file));
    }

    class CompressedWriteStore : public Store {
    private:
        BufferedWriterHelper writer_helper_;
        logream::WriterCompress writer_;

    public:
        explicit CompressedWriteStore(std::unique_ptr<penv::WritableFile> && file)
                : writer_helper_(std::move(file)),
                  writer_(&writer_helper_, 0) {}

        ~CompressedWriteStore() override = default;

    public:
        size_t Add(const Slice & s, bool sync) override {
            size_t n = s.size();
            return writer_.Add(s.data(), &n);
        }

        size_t Get(size_t id, std::string * s) const override {
            assert(false);
            return 0;
        }
    };

    std::unique_ptr<Store>
    Store::OpenForCompressedWrite(const std::string & fname) {
        auto file = penv::Env::Default()->OpenWritableFile(fname);
        file->Hint(penv::WritableFile::WLTH_SHORT);
        return std::make_unique<CompressedWriteStore>(std::move(file));
    }
}