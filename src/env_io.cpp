#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/exception.h"
#include "config.h"
#include "env_io.h"

#if defined(OS_MACOSX)
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX)
#define fdatasync fsync
#endif

#define error_info strerror(errno)

namespace levidb8 {
    namespace env_io {
        uint64_t getFileSize(const std::string & fname) {
            struct stat sbuf{};
            if (stat(fname.c_str(), &sbuf) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
            return static_cast<uint64_t>(sbuf.st_size);
        };

        bool fileExist(const std::string & fname) noexcept {
            return access(fname.c_str(), F_OK) == 0;
        };

        void deleteFile(const std::string & fname) {
            if (unlink(fname.c_str()) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
        };

        void renameFile(const std::string & fname, const std::string & target) {
            if (rename(fname.c_str(), target.c_str()) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
        };

        std::vector<std::string>
        getChildren(const std::string & dirname) {
            DIR * d = opendir(dirname.c_str());
            if (d == nullptr) {
                throw Exception::IOErrorException(dirname, error_info);
            }

            std::vector<std::string> res;
            struct dirent * entry{};
            while ((entry = readdir(d)) != nullptr) {
                if (not(entry->d_name[0] == '.'
                        && (entry->d_name[1] == '\0' // "."
                            || (entry->d_name[1] == '.' && entry->d_name[2] == '\0')))) { // ".."
                    res.emplace_back(entry->d_name);
                }
            }
            closedir(d);
            return res;
        }

        void createDir(const std::string & dirname) {
            if (mkdir(dirname.c_str(), 0755/* 权限 */) != 0) {
                throw Exception::IOErrorException(dirname, error_info);
            }
        }

        void deleteDir(const std::string & dirname) {
            if (rmdir(dirname.c_str()) != 0) {
                throw Exception::IOErrorException(dirname, error_info);
            }
        }
    }

    FileOpen::FileOpen(const std::string & fname, env_io::OpenMode mode) {
        int arg{};
        switch (mode) {
            case env_io::R_M:
                arg = O_RDONLY;
                break;
            case env_io::W_M:
                arg = O_WRONLY | O_CREAT | O_TRUNC;
                break;
            case env_io::A_M:
                arg = O_WRONLY | O_CREAT | O_APPEND;
                break;
            case env_io::RP_M:
                arg = O_RDWR;
                break;
            case env_io::WP_M:
                arg = O_RDWR | O_CREAT | O_TRUNC;
                break;
            case env_io::AP_M:
                arg = O_RDWR | O_CREAT | O_APPEND;
                break;
        }

        switch (mode) {
            case env_io::W_M:
            case env_io::A_M:
            case env_io::WP_M:
            case env_io::AP_M:
                _fd = open(fname.c_str(), arg, 0644/* 权限 */);
                break;
            default:
                _fd = open(fname.c_str(), arg);
                break;
        }
        if (_fd < 0) {
            throw Exception::IOErrorException(fname, error_info);
        }
    }

    FileOpen::~FileOpen() noexcept {
        if (_fd > 0) {
            close(_fd);
        }
    }

    FileFopen::FileFopen(const std::string & fname, env_io::OpenMode mode) {
        const char * arg{};
        switch (mode) {
            case env_io::R_M:
                arg = "r";
                break;
            case env_io::W_M:
                arg = "w";
                break;
            case env_io::A_M:
                arg = "a";
                break;
            case env_io::RP_M:
                arg = "r+";
                break;
            case env_io::WP_M:
                arg = "w+";
                break;
            case env_io::AP_M:
                arg = "a+";
                break;
        }
        _f = fopen(fname.c_str(), arg);
        if (_f == nullptr) {
            throw Exception::IOErrorException(fname, error_info);
        }
    }

    FileFopen::~FileFopen() noexcept {
        if (_f != nullptr) {
            fclose(_f);
        }
    }

    MmapFile::MmapFile(std::string fname)
            : _file(fname, env_io::fileExist(fname) ? env_io::RP_M : env_io::WP_M),
              _length(env_io::getFileSize(fname)),
              _filename(std::move(fname)) {
        if (_length == 0) { // 0 长度文件 mmap 会报错
            _length = kPageSize;
            if (ftruncate(_file._fd, static_cast<off_t>(_length)) != 0) {
                throw Exception::IOErrorException(_filename, error_info);
            }
        }
        _mmaped_region = mmap(nullptr, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
        if (_mmaped_region == MAP_FAILED) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    MmapFile::~MmapFile() noexcept {
        munmap(_mmaped_region, _length);
    }

    void MmapFile::grow() {
        const uint64_t length = _length;
        _length *= 2;
        if (ftruncate(_file._fd, static_cast<off_t>(_length)) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        // 主要运行在 linux 上, 每次扩容都要 munmap/mmap 的问题可以用 mremap 规避
#ifndef __linux__
        if (munmap(_mmaped_region, length) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        _mmaped_region = mmap(nullptr, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
#else
        _mmaped_region = mremap(_mmaped_region, length, _length, MREMAP_MAYMOVE);
#endif
        if (_mmaped_region == MAP_FAILED) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    void MmapFile::sync() {
        if (msync(_mmaped_region, _length, MS_SYNC) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        };
    }

    AppendableFile::AppendableFile(std::string fname)
            : _ffile(fname, env_io::A_M),
              _length(env_io::getFileSize(fname)),
              _filename(std::move(fname)) {}

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

    RandomAccessFile::RandomAccessFile(std::string fname)
            : _file(fname, env_io::R_M), _filename(std::move(fname)) {}

    Slice RandomAccessFile::read(uint64_t offset, size_t n, char * scratch) const {
        ssize_t r = pread(_file._fd, scratch, n, static_cast<off_t>(offset));
        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        return {scratch, static_cast<size_t>(r)};
    }

    RandomWriteFile::RandomWriteFile(std::string fname)
            : _file(fname, env_io::fileExist(fname) ? env_io::RP_M : env_io::WP_M),
              _filename(std::move(fname)) {}

    void RandomWriteFile::write(uint64_t offset, const Slice & data) {
        ssize_t r = pwrite(_file._fd, data.data(), data.size(), static_cast<off_t>(offset));
        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    SequentialFile::SequentialFile(std::string fname)
            : _ffile(fname, env_io::R_M), _filename(std::move(fname)) {}

    Slice SequentialFile::read(size_t n, char * scratch) {
        size_t r = fread_unlocked(scratch, 1, n, _ffile._f);
        if (r < n) {
            if (static_cast<bool>(feof(_ffile._f))) {
            } else {
                throw Exception::IOErrorException(_filename, error_info);
            }
        }
        return {scratch, r};
    }

    Slice SequentialFile::readLine() {
        char * line = nullptr;
        size_t len = 0;
        ssize_t read = getline(&line, &len, _ffile._f);

        if (read != -1) {
            return Slice::pinnableSlice(line, static_cast<size_t>(read));
        }
        if (static_cast<bool>(feof(_ffile._f))) {
            return {};
        }
        throw Exception::IOErrorException(_filename, error_info);
    }
}