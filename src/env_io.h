#ifndef LEVIDB_ENV_IO_H
#define LEVIDB_ENV_IO_H

/*
 * 封装 POSIX C IO API
 * 出于性能方面的考量, 放弃 C++ 的 IO 标准库
 *
 * steal from leveldb
 */

#include <sys/mman.h>
#include <unistd.h>
#include <vector>

#include "slice.h"
#include "util.h"

namespace LeviDB {
    namespace IOEnv {
        enum OpenMode {
            R_M,
            W_M,
            A_M,
            RP_M,
            WP_M,
            AP_M,
        };
        static constexpr int page_size_ = 4 * 1024; // 4KB

        uint64_t getFileSize(const std::string & fname);

        bool fileExists(const std::string & fname) noexcept;

        void deleteFile(const std::string & fname);

        void renameFile(const std::string & fname, const std::string & target);

        void truncateFile(const std::string & fname, uint64_t length);

        std::vector<std::string>
        getChildren(const std::string & dirname);

        void createDir(const std::string & dirname);

        void deleteDir(const std::string & dirname);
    }

    class FileOpen {
    public:
        int _fd;

        FileOpen(const std::string & fname, IOEnv::OpenMode mode);

        ~FileOpen() noexcept { if (_fd > 0) close(_fd); };

        DEFAULT_MOVE(FileOpen);
        DELETE_COPY(FileOpen);
    };

    class FileFopen {
    public:
        FILE * _f;

        FileFopen(const std::string & fname, IOEnv::OpenMode mode);

        ~FileFopen() noexcept { if (_f != nullptr) fclose(_f); }

        DEFAULT_MOVE(FileFopen);
        DELETE_COPY(FileFopen);
    };

    class MmapFile {
    private:
        void * _mmaped_region;
        std::string _filename;
        FileOpen _file;
        uint64_t _length;

    public:
        explicit MmapFile(std::string fname);

        ~MmapFile() noexcept { munmap(_mmaped_region, _length); }

        void grow();

        void sync();

        EXPOSE(_mmaped_region);

        EXPOSE(_length);

        DEFAULT_MOVE(MmapFile);
        DELETE_COPY(MmapFile);
    };

    class AppendableFile {
    private:
        std::string _filename;
        FileFopen _ffile;
        uint64_t _length;

    public:
        explicit AppendableFile(std::string fname);

        ~AppendableFile() noexcept = default;

        void append(const Slice & data);

        void flush();

        void sync();

        EXPOSE(_length);

        DEFAULT_MOVE(AppendableFile);
        DELETE_COPY(AppendableFile);
    };

    class RandomAccessFile {
    private:
        std::string _filename;
        FileOpen _file;

    public:
        explicit RandomAccessFile(std::string fname);

        ~RandomAccessFile() noexcept = default;

        Slice read(uint64_t offset, size_t n, char * scratch) const;

        DEFAULT_MOVE(RandomAccessFile);
        DELETE_COPY(RandomAccessFile);
    };

    class RandomWriteFile {
    private:
        std::string _filename;
        FileOpen _file;

    public:
        explicit RandomWriteFile(std::string fname);

        ~RandomWriteFile() noexcept = default;

        void write(uint64_t offset, const Slice & data);

        void sync();

        DEFAULT_MOVE(RandomWriteFile);
        DELETE_COPY(RandomWriteFile);
    };

    class SequentialFile {
    private:
        std::string _filename;
        FileFopen _ffile;

    public:
        explicit SequentialFile(std::string fname);

        ~SequentialFile() noexcept = default;

        Slice read(size_t n, char * scratch);

        void skip(uint64_t offset);

        std::string readLine();

        DEFAULT_MOVE(SequentialFile);
        DELETE_COPY(SequentialFile);
    };

    // 记录人类可读的日志
    class Logger {
    private:
        FileFopen _ffile;

    public:
        explicit Logger(const std::string & fname);

        ~Logger() noexcept = default;

        void logv(const char * format, va_list ap) noexcept;

        DEFAULT_MOVE(Logger);
        DELETE_COPY(Logger);

    public:
        static void logForMan(Logger * info_log, const char * format, ...) noexcept
        __attribute__((__format__ (__printf__, 2, 3)));
    };

    class FileLock {
    private:
        FileOpen _file;

    public:
        explicit FileLock(const std::string & fname);

        ~FileLock() noexcept;

        DEFAULT_MOVE(FileLock);
        DELETE_COPY(FileLock);
    };
}

#endif //LEVIDB_ENV_IO_H