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
        explicit MmapFile(const std::string & fname);

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
        explicit AppendableFile(const std::string & fname);

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
        explicit RandomAccessFile(const std::string & fname);

        ~RandomAccessFile() noexcept = default;

        Slice read(uint64_t offset, size_t n, char * scratch) const;

        DEFAULT_MOVE(RandomAccessFile);
        DELETE_COPY(RandomAccessFile);
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

        std::string readLine();

        DEFAULT_MOVE(SequentialFile);
        DELETE_COPY(SequentialFile);
    };
}

#endif //LEVIDB_ENV_IO_H