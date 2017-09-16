#include <cerrno>
#include <cstdarg>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "env_io.h"
#include "env_thread.h"
#include "exception.h"

#if defined(OS_MACOSX)
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX)
#define fdatasync fsync
#endif

#define error_info strerror(errno)

namespace LeviDB {
    namespace IOEnv {
        uint64_t getFileSize(const std::string & fname) {
            struct stat sbuf{};
            if (stat(fname.c_str(), &sbuf) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
            return static_cast<uint64_t>(sbuf.st_size);
        };

        bool fileExists(const std::string & fname) noexcept {
            return access(fname.c_str(), F_OK) == 0;
        }

        void deleteFile(const std::string & fname) {
            if (unlink(fname.c_str()) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
        }

        void renameFile(const std::string & fname, const std::string & target) {
            if (rename(fname.c_str(), target.c_str()) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
        }

        void truncateFile(const std::string & fname, uint64_t length) {
            if (truncate(fname.c_str(), static_cast<off_t>(length)) != 0) {
                throw Exception::IOErrorException(fname, error_info);
            }
        }

        std::vector<std::string>
        getChildren(const std::string & dirname) {
            std::vector<std::string> res;
            DIR * d = opendir(dirname.c_str());
            if (d == nullptr) {
                throw Exception::IOErrorException(dirname, error_info);
            }
            struct dirent * entry{};
            while ((entry = readdir(d)) != nullptr) {
                if (entry->d_name[0] != '.') {
                    res.emplace_back(entry->d_name);
                }
            }
            closedir(d);
            return res;
        };

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

        int fd;
        switch (mode) {
            case IOEnv::W_M:
            case IOEnv::A_M:
            case IOEnv::WP_M:
            case IOEnv::AP_M:
                fd = open(fname.c_str(), arg, 0644/* 权限 */);
                break;
            default:
                fd = open(fname.c_str(), arg);
                break;
        }
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
        if (f == nullptr) {
            throw Exception::IOErrorException(fname, error_info);
        }
        _f = f;
    }

    MmapFile::MmapFile(std::string fname)
            : _filename(std::move(fname)),
              _file(_filename, IOEnv::fileExists(_filename) ? IOEnv::RP_M : IOEnv::WP_M),
              _length(IOEnv::getFileSize(_filename)) {
        if (_length == 0) { // 0 长度文件 mmap 会报错
            _length = IOEnv::page_size_;
            if (ftruncate(_file._fd, static_cast<off_t>(_length)) != 0) {
                throw Exception::IOErrorException(_filename, error_info);
            }
        }
        _mmaped_region = mmap(nullptr, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
        if (_mmaped_region == MAP_FAILED) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    void MmapFile::grow() {
        _length += IOEnv::page_size_;
        if (ftruncate(_file._fd, static_cast<off_t>(_length)) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        // 主要运行在 linux 上, 每次扩容都要 munmap/mmap 的问题可以用 mremap 规避
#ifndef __linux__
        if (munmap(_mmaped_region, _length - IOEnv::page_size_) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        _mmaped_region = mmap(nullptr, _length, PROT_READ | PROT_WRITE, MAP_SHARED, _file._fd, 0);
#else
        _mmaped_region = mremap(_mmaped_region, _length - IOEnv::page_size_, _length, MREMAP_MAYMOVE);
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
            : _filename(std::move(fname)), _ffile(_filename, IOEnv::A_M), _length(IOEnv::getFileSize(_filename)) {}

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
            : _filename(std::move(fname)), _file(_filename, IOEnv::R_M) {}

    Slice RandomAccessFile::read(uint64_t offset, size_t n, char * scratch) const {
        ssize_t r = pread(_file._fd, scratch, n, static_cast<off_t>(offset));
        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
        return {scratch, static_cast<size_t>(r)};
    }

    RandomWriteFile::RandomWriteFile(std::string fname)
            : _filename(std::move(fname)), _file(_filename, IOEnv::fileExists(_filename) ? IOEnv::RP_M : IOEnv::WP_M) {}

    void RandomWriteFile::write(uint64_t offset, const Slice & data) {
        ssize_t r = pwrite(_file._fd, data.data(), data.size(), static_cast<off_t>(offset));
        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    void RandomWriteFile::sync() {
        if (fdatasync(_file._fd) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    SequentialFile::SequentialFile(std::string fname)
            : _filename(std::move(fname)), _ffile(_filename, IOEnv::R_M) {}

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

    void SequentialFile::skip(uint64_t offset) {
        if (fseek(_ffile._f, static_cast<long>(offset), SEEK_CUR) != 0) {
            throw Exception::IOErrorException(_filename, error_info);
        }
    }

    std::string SequentialFile::readLine() {
        char * line = nullptr;
        size_t len = 0;
        ssize_t read = getline(&line, &len, _ffile._f);

        std::string res;
        if (read != -1) {
            res = {line, static_cast<size_t>(read)};
        }
        free(line);
        return res;
    }

    Logger::Logger(const std::string & fname) : _ffile(fname, IOEnv::W_M) {}

    void Logger::logv(const char * format, va_list ap) noexcept {
        uint64_t thread_id = ThreadEnv::gettid();

        char buffer[500];
        std::unique_ptr<char[]> tmp;
        for (const int iter :{0, 1}) {
            char * base;
            int buff_size;
            if (iter == 0) {
                buff_size = sizeof(buffer);
                base = buffer;
            } else {
                assert(iter == 1);
                buff_size = 30000;
                tmp = std::unique_ptr<char[]>(new char[buff_size]);
                base = tmp.get();
            }
            char * p = base;
            char * limit = base + buff_size;

            struct timeval now_tv{};
            gettimeofday(&now_tv, nullptr);
            const time_t seconds = now_tv.tv_sec;
            struct tm t{};
            localtime_r(&seconds, &t);
            p += snprintf(p, limit - p,
                          "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                          t.tm_year + 1900,
                          t.tm_mon + 1,
                          t.tm_mday,
                          t.tm_hour,
                          t.tm_min,
                          t.tm_sec,
                          static_cast<int>(now_tv.tv_usec),
                          static_cast<long long unsigned int>(thread_id));

            // 写入
            if (p < limit) {
                va_list backup_ap;
                va_copy(backup_ap, ap);
                p += vsnprintf(p, limit - p, format, backup_ap);
                va_end(backup_ap);
            }

            if (p >= limit) {
                if (iter == 0) {
                    continue;
                }
                p = limit - 1;
            }
            if (p == base || p[-1] != '\n') {
                *p++ = '\n';
            }

            assert(p <= limit);
            fwrite(base, 1, p - base, _ffile._f);
            fflush(_ffile._f);
            break;
        }
    }

    void Logger::logForMan(Logger * info_log, const char * format, ...) noexcept {
        assert(info_log != nullptr);
        va_list ap;
        va_start(ap, format);
        info_log->logv(format, ap);
        va_end(ap);
    }

    template<bool LOCK>
    static int lockOrUnlock(int fd) noexcept {
        struct flock f{};
        f.l_type = static_cast<short>(LOCK ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;
        return fcntl(fd, F_SETLK, &f);
    }

    FileLock::FileLock(const std::string & fname) : _file(fname, IOEnv::WP_M) {
        if (lockOrUnlock<true>(_file._fd) == -1) {
            throw Exception::IOErrorException("lock " + fname, error_info);
        }
    }

    FileLock::~FileLock() noexcept {
        lockOrUnlock<false>(_file._fd);
    }
}