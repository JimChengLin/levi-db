#include "env.h"

namespace LeviDB {
    void log4Man(Logger & info_log, const char * format, ...) noexcept {
        va_list ap;
        va_start(ap, format);
        info_log.logv(format, ap);
        va_end(ap);
    };

    template<bool should_sync>
    static void doWriteStringToFile(const Slice & data, const std::string & fname,
                                    bool should_sync) {
        try {
            auto file = IOEnv::newWritableFile(fname);
            file->append(data);
            if (should_sync) {
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
        doWriteStringToFile(data, fname, false);
    };

    void writeStringToFileSync(const Slice & data, const std::string & fname) {
        doWriteStringToFile(data, fname, true);
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

}