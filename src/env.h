#ifndef LEVIDB_ENV_H
#define LEVIDB_ENV_H

/*
 * borrow from leveldb
 * 负责所有系统底层交互
 */

#include "slice.h"
#include <cstdio>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

namespace LeviDB {
    class SequentialFile;

    class RandomAccessFile;

    class WritableFile;

    class Logger;

    class FileLock;

    namespace IOEnv {
        std::unique_ptr<SequentialFile> newSequentialFile(const std::string & fname);

        std::unique_ptr<RandomAccessFile> newRandomAccessFile(const std::string & fname);

        std::unique_ptr<WritableFile> newWritableFile(const std::string & fname);

        std::unique_ptr<WritableFile> newAppendableFile(const std::string & fname);

        std::unique_ptr<Logger> newLogger(const std::string & fname);

        std::unique_ptr<FileLock> newlockFile(const std::string & fname);

        bool fileExists(const std::string & fname) noexcept;

        std::vector<std::string> getChildren(const std::string & dir);

        void deleteFile(const std::string & fname);

        void createDir(const std::string & dirname);

        void deleteDir(const std::string & dirname);

        uint64_t getFileSize(const std::string & fname);

        void renameFile(const std::string & src, const std::string & target);

        void unlockFile(FileLock * lock);
    }; //namespace IOEnv

    class SequentialFile {
    private:
        std::string _filename;
        FILE * _file;

    public:
        SequentialFile(const std::string & fname, FILE * f) noexcept
                : _filename(fname), _file(f) {};

        ~SequentialFile() noexcept { fclose(_file); };

        Slice read(size_t n, char * scratch);

        void skip(uint64_t n);

    private:
        // 禁止复制
        SequentialFile(const SequentialFile &);

        void operator=(const SequentialFile &);
    };

    class RandomAccessFile {
    private:
        std::string _filename;
        int _fd;

    public:
        RandomAccessFile(const std::string & fname, int fd) noexcept
                : _filename(fname), _fd(fd) {};

        ~RandomAccessFile() noexcept { close(_fd); };

        Slice read(uint64_t offset, size_t n, char * scratch);

    private:
        // 禁止复制
        RandomAccessFile(const RandomAccessFile &);

        void operator=(const RandomAccessFile &);
    };

    class WritableFile {
    private:
        std::string _filename;
        FILE * _file;

    public:
        WritableFile(const std::string & fname, FILE * f) noexcept
                : _filename(fname), _file(f) {};

        ~WritableFile() noexcept { fclose(_file); };

        void append(const Slice & data);

        void flush();

        void sync();

    private:
        // 禁止复制
        WritableFile(const WritableFile &);

        void operator=(const WritableFile &);

        void syncDirIfManifest();
    };

    class Logger {
    private:
        FILE * _file;

    public:
        Logger(FILE * f) noexcept : _file(f) {};

        ~Logger() noexcept { fclose(_file); };

        void logv(const char * format, va_list ap) noexcept;

    private:
        // 禁止复制
        Logger(const Logger &);

        void operator=(const Logger &);
    };

    class FileLock {
    public:
        std::string _filename;
        int _fd;

    public:
        FileLock() noexcept {};

        ~FileLock() noexcept {};

    private:
        // 禁止复制
        FileLock(const FileLock &);

        void operator=(const FileLock &);
    };

    void log4Man(Logger * info_log, const char * format, ...) noexcept
    __attribute__((__format__ (__printf__, 2, 3)));

    void writeStringToFile(const Slice & data, const std::string & fname);

    void writeStringToFileSync(const Slice & data, const std::string & fname);

    void ReadFileToString(const std::string & fname, std::string & data);

    namespace ThreadEnv {
        uint64_t gettid() noexcept;
    }
} //namespace LeviDB

#endif //LEVIDB_ENV_H