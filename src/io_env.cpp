#include "env.h"
#include "exception.h"
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <thread>

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

#define str_error strerror(errno)

namespace LeviDB {
    void log4Man(Logger * info_log, const char * format, ...) noexcept {
        assert(info_log != nullptr);

        va_list ap;
        va_start(ap, format);
        info_log->logv(format, ap);
        va_end(ap);
    };

    template<bool SYNC>
    static void doWriteStringToFile(const Slice & data, const std::string & fname) {
        try {
            auto file = IOEnv::newWritableFile(fname);
            file->append(data);
            if (SYNC) {
                file->sync();
            }
        } catch (const std::exception & e) {
            try {
                IOEnv::deleteFile(fname);
            } catch (const std::exception & _) {
            }
            throw;
        }
    }

    void writeStringToFile(const Slice & data, const std::string & fname) {
        doWriteStringToFile<false>(data, fname);
    };

    void writeStringToFileSync(const Slice & data, const std::string & fname) {
        doWriteStringToFile<true>(data, fname);
    };

    std::string ReadFileToString(const std::string & fname) {
        std::string data;
        auto file = IOEnv::newSequentialFile(fname);

        static constexpr int buffer_size = 8192;
        char space[buffer_size];

        while (true) {
            Slice fragment = file->read(buffer_size, space);
            data.append(fragment.data(), fragment.size());
            if (fragment.empty()) {
                break;
            }
        }
        return data;
    };

    Slice SequentialFile::read(size_t n, char * scratch) {
        size_t r = fread_unlocked(scratch, 1, n, _file);
        if (r < n) {
            if (feof(_file)) {
                return Slice(scratch, r);
            } else {
                throw Exception::IOErrorException(_filename, str_error);
            }
        }
        return Slice(scratch, r);
    }

    void SequentialFile::skip(uint64_t n) {
        if (fseek(_file, static_cast<long>(n), SEEK_CUR)) {
            throw Exception::IOErrorException(_filename, str_error);
        }
    }

    Slice RandomAccessFile::read(uint64_t offset, size_t n, char * scratch) {
        ssize_t r = pread(_fd, scratch, n, static_cast<off_t>(offset));
        Slice res(scratch, static_cast<size_t>((r < 0) ? 0 : r));

        if (r < 0) {
            throw Exception::IOErrorException(_filename, str_error);
        }
        return res;
    }

    void WritableFile::append(const Slice & data) {
        size_t r = fwrite_unlocked(data.data(), 1, data.size(), _file);
        if (r != data.size()) {
            throw Exception::IOErrorException(_filename, str_error);
        }
    }

    void WritableFile::flush() {
        if (fflush_unlocked(_file) != 0) {
            throw Exception::IOErrorException(_filename, str_error);
        }
    }

    void WritableFile::sync() {
        syncDirIfManifest();
        if (fflush_unlocked(_file) != 0 || fdatasync(fileno(_file)) != 0) {
            throw Exception::IOErrorException(_filename, str_error);
        }
    }

    void WritableFile::syncDirIfManifest() {
        const char * f = _filename.c_str();
        const char * sep = strrchr(f, '/');
        Slice basename;
        std::string dir;
        if (sep == NULL) {
            dir = ".";
            basename = f;
        } else {
            dir = std::string(f, sep - f);
            basename = sep + 1;
        }
        if (basename.startsWith("MANIFEST")) {
            int fd = open(dir.c_str(), O_RDONLY);
            if (fd < 0) {
                throw Exception::IOErrorException(dir, str_error);
            } else {
                if (fsync(fd) < 0) {
                    throw Exception::IOErrorException(dir, str_error);
                }
                close(fd);
            }
        }
    }

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

            struct timeval now_tv;
            gettimeofday(&now_tv, NULL);
            const time_t seconds = now_tv.tv_sec;
            struct tm t;
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
                } else {
                    p = limit - 1;
                }
            }
            if (p == base || p[-1] != '\n') {
                *p++ = '\n';
            }

            assert(p <= limit);
            fwrite(base, 1, p - base, _file);
            fflush(_file);
            break;
        }
    }

    template<bool LOCK>
    static int lockOrUnlock(int fd) noexcept {
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = static_cast<short>(LOCK ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;
        return fcntl(fd, F_SETLK, &f);
    }

    namespace IOEnv {
        std::unique_ptr<SequentialFile> newSequentialFile(const std::string & fname) {
            FILE * f = fopen(fname.c_str(), "r");
            if (f == NULL) {
                throw Exception::IOErrorException(fname, str_error);
            }
            return std::make_unique<SequentialFile>(fname, f);
        };

        std::unique_ptr<RandomAccessFile> newRandomAccessFile(const std::string & fname) {
            int fd = open(fname.c_str(), O_RDONLY);
            if (fd < 0) {
                throw Exception::IOErrorException(fname, str_error);
            }
            return std::make_unique<RandomAccessFile>(fname, fd);
        };

        std::unique_ptr<WritableFile> newWritableFile(const std::string & fname) {
            FILE * f = fopen(fname.c_str(), "w");
            if (f == NULL) {
                throw Exception::IOErrorException(fname, str_error);
            }
            return std::make_unique<WritableFile>(fname, f);
        };

        std::unique_ptr<WritableFile> newAppendableFile(const std::string & fname) {
            FILE * f = fopen(fname.c_str(), "a");
            if (f == NULL) {
                throw Exception::IOErrorException(fname, str_error);
            }
            return std::make_unique<WritableFile>(fname, f);
        };

        std::unique_ptr<Logger> newLogger(const std::string & fname) {
            FILE * f = fopen(fname.c_str(), "w");
            if (f == NULL) {
                throw Exception::IOErrorException(fname.c_str(), str_error);
            }
            return std::make_unique<Logger>(f);
        };

        bool fileExists(const std::string & fname) noexcept {
            return access(fname.c_str(), F_OK) == 0;
        };

        std::vector<std::string> getChildren(const std::string & dir) {
            std::vector<std::string> res;

            DIR * d = opendir(dir.c_str());
            if (d == NULL) {
                throw Exception::IOErrorException(dir, str_error);
            }

            struct dirent * entry;
            while ((entry = readdir(d)) != NULL) {
                res.push_back(std::string(entry->d_name));
            }
            closedir(d);

            return res;
        };

        void deleteFile(const std::string & fname) {
            if (unlink(fname.c_str()) != 0) {
                throw Exception::IOErrorException(fname, str_error);
            }
        };

        void createDir(const std::string & dirname) {
            if (mkdir(dirname.c_str(), 0755) != 0) {
                throw Exception::IOErrorException(dirname, str_error);
            }
        };

        void deleteDir(const std::string & dirname) {
            if (rmdir(dirname.c_str()) != 0) {
                throw Exception::IOErrorException(dirname, str_error);
            }
        };

        uint64_t getFileSize(const std::string & fname) {
            struct stat sbuf;
            if (stat(fname.c_str(), &sbuf) != 0) {
                throw Exception::IOErrorException(fname, str_error);
            }
            return static_cast<uint64_t>(sbuf.st_size);
        };

        void renameFile(const std::string & src, const std::string & target) {
            if (rename(src.c_str(), target.c_str()) != 0) {
                throw Exception::IOErrorException(src, str_error);
            }
        };

        std::unique_ptr<FileLock> lockFile(const std::string & fname) {
            int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd < 0) {
                throw Exception::IOErrorException(fname, str_error);
            } else if (lockOrUnlock<true>(fd) == -1) {
                close(fd);
                throw Exception::IOErrorException("lock " + fname, str_error);
            } else {
                return std::make_unique<FileLock>(fname, fd);
            }
        };

        void unlockFile(FileLock * lock) {
            if (lockOrUnlock<false>(lock->_fd) == -1) {
                throw Exception::IOErrorException("unlock", str_error);
            }
        };
    }
}