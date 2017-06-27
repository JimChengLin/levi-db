#ifndef LEVIDB_ENV_H
#define LEVIDB_ENV_H

#include "slice.h"
#include <memory>
#include <string>
#include <vector>

namespace LeviDB {
    namespace IOEnv {
        std::unique_ptr<SequentialFile> newSequentialFile(const std::string & fname);

        std::unique_ptr<RandomAccessFile> newRandomAccessFile(const std::string & fname);

        std::unique_ptr<WritableFile> newWritableFile(const std::string & fname);

        std::unique_ptr<WritableFile> newAppendableFile(const std::string & fname);

        bool fileExists(const std::string & fname) noexcept;

        std::vector<std::string> getChildren(const std::string & dir);

        void deleteFile(const std::string & fname);

        void createDir(const std::string & dirname);

        void deleteDir(const std::string & dirname);

        uint64_t getFileSize(const std::string & fname);

        void renameFile(const std::string & src, const std::string & target);

        std::unique_ptr<FileLock> lockFile(const std::string & fname);

        void unlockFile(FileLock * lock);

        std::unique_ptr<Logger> newLogger(const std::string & fname);
    }; //namespace IOEnv

    class SequentialFile {
    public:
        SequentialFile() noexcept {};

        ~SequentialFile() noexcept;

        Slice read(size_t n, char * scratch);

        void skip(uint64_t n);

    private:
        SequentialFile(const SequentialFile &);

        void operator=(const SequentialFile &);
    };

    class RandomAccessFile {
        RandomAccessFile() noexcept {};

        ~RandomAccessFile() noexcept;

        Slice read(uint64_t offset, size_t n, char * scratch);

    private:
        RandomAccessFile(const RandomAccessFile &);

        void operator=(const RandomAccessFile &);
    };

    class WritableFile {
    public:
        WritableFile() noexcept {};

        ~WritableFile();

        void append(const Slice & data);

        void flush();

        void sync();

    private:
        WritableFile(const WritableFile &);

        void operator=(const WritableFile &);
    };

    class Logger {
    public:
        Logger() noexcept {};

        ~Logger() noexcept;

        void logv(const char * format, va_list ap) noexcept;

    private:
        Logger(const Logger &);

        void operator=(const Logger &);
    };

    class FileLock {
    public:
        FileLock() {};

        ~FileLock();

    private:
        FileLock(const FileLock &);

        void operator=(const FileLock &);
    };

    void log4Man(Logger & info_log, const char * format, ...) noexcept
    __attribute__((__format__ (__printf__, 2, 3)));

    void writeStringToFile(const Slice & data, const std::string & fname);

    void writeStringToFileSync(const Slice & data, const std::string & fname);

    void ReadFileToString(const std::string & fname, std::string & data);
} //namespace LeviDB

#endif //LEVIDB_ENV_H