#pragma once
#ifndef LEVIDB8_ENV_IO_H
#define LEVIDB8_ENV_IO_H

/*
 * 封装 POSIX C IO API
 */

#include <vector>

#include "../include/slice.h"
#include "util.h"

namespace levidb8 {
    namespace env_io {
        enum OpenMode {
            R_M,
            W_M,
            A_M,
            RP_M,
            WP_M,
            AP_M,
        };

        uint64_t getFileSize(const std::string & fname);

        bool fileExist(const std::string & fname) noexcept;

        void deleteFile(const std::string & fname);

        void renameFile(const std::string & fname, const std::string & target);

        std::vector<std::string>
        getChildren(const std::string & dirname);

        void createDir(const std::string & dirname);

        void deleteDir(const std::string & dirname);
    }

    class FileOpen {
    public:
        int _fd;

        FileOpen(const std::string & fname, env_io::OpenMode mode);

        ~FileOpen() noexcept;
    };

    class FileFopen {
    public:
        FILE * _f;

        FileFopen(const std::string & fname, env_io::OpenMode mode);

        ~FileFopen() noexcept;
    };

    class MmapFile {
    private:
        void * _mmaped_region;
        FileOpen _file;
        uint64_t _length;
        std::string _filename;

    public:
        explicit MmapFile(std::string fname);

        ~MmapFile() noexcept;

    public:
        EXPOSE(_mmaped_region);

        EXPOSE(_length);

        void grow();

        void sync();
    };

    class AppendableFile {
    private:
        FileFopen _ffile;
        uint64_t _length;
        std::string _filename;

    public:
        explicit AppendableFile(std::string fname);

        EXPOSE(_length);

        void append(const Slice & data);

        void flush();

        void sync();
    };

    class RandomAccessFile {
    private:
        FileOpen _file;
        std::string _filename;

    public:
        explicit RandomAccessFile(std::string fname);

        Slice read(uint64_t offset, size_t n, char * scratch) const;
    };

    class RandomWriteFile {
    private:
        FileOpen _file;
        std::string _filename;

    public:
        explicit RandomWriteFile(std::string fname);

        void write(uint64_t offset, const Slice & data);
    };

    class SequentialFile {
    private:
        FileFopen _ffile;
        std::string _filename;

    public:
        explicit SequentialFile(std::string fname);

        Slice read(size_t n, char * scratch);

        Slice readLine();
    };
}

#endif //LEVIDB8_ENV_IO_H
