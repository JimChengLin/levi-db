#include "env_io.h"
#include "exception.h"
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) || \
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) || \
    defined(OS_ANDROID) || defined(OS_HPUX) || defined(CYGWIN)
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) || \
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
#define fdatasync fsync
#endif

#define error_info strerror(errno)

namespace LeviDB {
    namespace IOEnv {
        uint64_t getFileSize(const std::string & fname) {
            struct stat sbuf;
            if (stat(fname.c_str(), &sbuf) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
            return static_cast<uint64_t>(sbuf.st_size);
        };
    }

    FileOpen::FileOpen(const std::string & fname, IOEnv::OpenMode mode) {
        int arg = 0;
        switch (mode) {
            case IOEnv::R_M:
                arg = O_RDONLY;
                break;
            case IOEnv::W_M:
                arg = O_WRONLY | O_CREAT | O_TRUNC;
                break;
            case IOEnv::A_M:
                arg = O_WRONLY | O_CREAT | O_APPEND;
                break;
            case IOEnv::RP_M:
                arg = O_RDWR;
                break;
            case IOEnv::WP_M:
                arg = O_RDWR | O_CREAT | O_TRUNC;
                break;
            case IOEnv::AP_M:
                arg = O_RDWR | O_CREAT | O_APPEND;
                break;
        }
        int fd = open(fname.c_str(), arg);
        if (fd < 0) {
            throw Exception::IOErrorException(fname, error_info);
        }
        _fd = fd;
    }

    FileFopen::FileFopen(const std::string & fname, IOEnv::OpenMode mode) {
        const char * arg = nullptr;
        switch (mode) {
            case IOEnv::R_M:
                arg = "r";
                break;
            case IOEnv::W_M:
                arg = "w";
                break;
            case IOEnv::A_M:
                arg = "a";
                break;
            case IOEnv::RP_M:
                arg = "r+";
                break;
            case IOEnv::WP_M:
                arg = "w+";
                break;
            case IOEnv::AP_M:
                arg = "a+";
                break;
        }
        FILE * f = fopen(fname.c_str(), arg);
        if (f == NULL) {
            throw Exception::IOErrorException(fname, error_info);
        }
        _f = f;
    }

    MmapFile::MmapFile(const std::string & fname)
            : _file(fname, IOEnv::WP_M), _length(IOEnv::getFileSize(fname)), _filename(fname) {
        _mmaped_region = mmap(NULL, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
        if (_mmaped_region == MAP_FAILED) {
            throw Exception::IOErrorException(fname, error_info);
        }
    }

    Slice MmapFile::read(uint64_t offset, size_t n) const noexcept {
        assert(offset + n <= _length);
        return Slice(reinterpret_cast<char *>(_mmaped_region) + offset, n);
    }

    void MmapFile::write(uint64_t offset, const Slice & data) {
        if (offset + data.size() > _length) {
            grow();
        }
        memcpy(reinterpret_cast<char *>(_mmaped_region) + offset, data.data(), data.size());
    }

    void MmapFile::grow() {
        _length += IOEnv::page_size;
        if (ftruncate(_file._fd, static_cast<off_t>(_length)) < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
#ifndef __linux__
        if (munmap(_mmaped_region, _length - IOEnv::page_size) < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        _mmaped_region = mmap(NULL, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
#else
        _mmaped_region = mremap(_mmaped_region, _length - IOEnv::page_size, _length, 0);
#endif
        if (_mmaped_region == MAP_FAILED) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    AppendableFile::AppendableFile(const std::string & fname)
            : _ffile(fname, IOEnv::A_M), _length(IOEnv::getFileSize(fname)), _filename(fname) {}

    void AppendableFile::append(const Slice & data) {
        size_t r = fwrite_unlocked(data.data(), 1, data.size(), _ffile._f);
        _length += r;
        if (r != data.size()) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    void AppendableFile::flush() {
        if (fflush_unlocked(_ffile._f) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    void AppendableFile::sync() {
        flush();
        if (fdatasync(fileno(_ffile._f)) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    RandomAccessFile::RandomAccessFile(const std::string & fname)
            : _file(fname, IOEnv::R_M), _filename(fname) {}

    Slice RandomAccessFile::read(uint64_t offset, size_t n, char * scratch) const {
        ssize_t r = pread(_file._fd, scratch, n, static_cast<off_t >(offset));
        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        return Slice(scratch, static_cast<size_t>(r));
    }
}