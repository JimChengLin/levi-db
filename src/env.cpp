#include "env.h"
#include "exception.h"
#include <cerrno>
#include <fcntl.h>
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

#define error_msg strerror(errno)

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

    void ReadFileToString(const std::string & fname, std::string & data) {
        data.clear();
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
    };

    Slice SequentialFile::read(size_t n, char * scratch) {
        size_t r = fread_unlocked(scratch, 1, n, _file);
        if (r < n) {
            if (feof(_file)) {
                return Slice(scratch, r);
            } else {
                throw Exception::IOErrorException(_filename, error_msg);
            }
        }
        return Slice(scratch, r);
    }

    void SequentialFile::skip(uint64_t n) {
        if (fseek(_file, static_cast<long>(n), SEEK_CUR)) {
            throw Exception::IOErrorException(_filename, error_msg);
        }
    }

    Slice RandomAccessFile::read(uint64_t offset, size_t n, char * scratch) {
        ssize_t r = pread(_fd, scratch, n, static_cast<off_t>(offset));
        Slice res(scratch, static_cast<size_t>((r < 0) ? 0 : r));

        if (r < 0) {
            throw Exception::IOErrorException(_filename, error_msg);
        }
        return res;
    }

    void WritableFile::append(const Slice & data) {
        size_t r = fwrite_unlocked(data.data(), 1, data.size(), _file);
        if (r != data.size()) {
            throw Exception::IOErrorException(_filename, error_msg);
        }
    }

    void WritableFile::flush() {
        if (fflush_unlocked(_file) != 0) {
            throw Exception::IOErrorException(_filename, error_msg);
        }
    }

    void WritableFile::sync() {
        syncDirIfManifest();
        if (fflush_unlocked(_file) != 0 || fdatasync(fileno(_file)) != 0) {
            throw Exception::IOErrorException(_filename, error_msg);
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
                throw Exception::IOErrorException(dir, error_msg);
            } else {
                if (fsync(fd) < 0) {
                    throw Exception::IOErrorException(dir, error_msg);
                }
                close(fd);
            }
        }
    }

    static int lockOrUnlock(int fd, bool lock) {
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = static_cast<short>(lock ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0; // Lock/unlock entire file
        return fcntl(fd, F_SETLK, &f);
    }

    void Logger::logv(const char * format, va_list ap) noexcept {

    }
}