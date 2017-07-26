#ifndef LEVIDB_ENV_IO_H
#define LEVIDB_ENV_IO_H

/*
 * IO API 封装
 */

#include "slice.h"
#include <string>
#include <sys/mman.h>
#include <unistd.h>

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
        static constexpr int page_size = 4 * 1024; // 4KB

        uint64_t getFileSize(const std::string & fname);

        bool fileExists(const std::string & fname) noexcept;

        void deleteFile(const std::string & fname);
    }

    class FileOpen {
    public:
        int _fd;

        FileOpen(const std::string & fname, IOEnv::OpenMode mode);

        ~FileOpen() noexcept { if (_fd > 0) close(_fd); };

        // 禁止复制
        FileOpen(const FileOpen &) = delete;

        void operator=(const FileOpen &) = delete;
    };

    class FileFopen {
    public:
        FILE * _f;

        FileFopen(const std::string & fname, IOEnv::OpenMode mode);

        ~FileFopen() noexcept { if (_f != nullptr) fclose(_f); }

        // 禁止复制
        FileFopen(const FileFopen &) = delete;

        void operator=(const FileFopen &) = delete;
    };

    class MmapFile {
    private:
        void * _mmaped_region;
        std::string _filename;
        FileOpen _file;
        uint64_t _length;

    public:
        explicit MmapFile(const std::string & fname);

        ~MmapFile() noexcept { munmap(_mmaped_region, _length); }

        Slice read(uint64_t offset, size_t n) const noexcept;

        void write(uint64_t offset, const Slice & data);

        void grow();

        // 禁止复制
        MmapFile(const MmapFile &) = delete;

        void operator=(const MmapFile &) = delete;
    };

    class AppendableFile {
    private:
        std::string _filename;
        FileFopen _ffile;
        uint64_t _length;

    public:
        explicit AppendableFile(const std::string & fname);

        ~AppendableFile() noexcept = default;

        void append(const Slice & data);

        void flush();

        void sync();

        uint64_t length() const noexcept { return _length; }

        // 禁止复制
        AppendableFile(const AppendableFile &) = delete;

        void operator=(const AppendableFile &) = delete;
    };

    class RandomAccessFile {
    private:
        std::string _filename;
        FileOpen _file;

    public:
        explicit RandomAccessFile(const std::string & fname);

        ~RandomAccessFile() noexcept = default;

        Slice read(uint64_t offset, size_t n, char * scratch) const;

        // 禁止复制
        RandomAccessFile(const RandomAccessFile &) = delete;

        void operator=(const RandomAccessFile &) = delete;
    };

    class SequentialFile {
    private:
        std::string _filename;
        FileFopen _ffile;

    public:
        explicit SequentialFile(const std::string & fname);

        ~SequentialFile() noexcept = default;

        Slice read(size_t n, char * scratch);

        void skip(uint64_t offset);

        FILE * getFILE() const noexcept { return _ffile._f; }
    };
}

#endif //LEVIDB_ENV_IO_H