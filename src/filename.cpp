#include <cassert>

#include "filename.h"

namespace levidb {
    static std::string_view GetFilename(const std::string & fname) {
        auto i = fname.rfind('/');
        if (i++ == std::string::npos) {
            return fname;
        }
        return {fname.data() + i, fname.size() - i};
    }

    static constexpr char kCompressedStoreSuffix[] = ".cprs";

    bool IsCompressedStore(const std::string & fname) {
        assert(IsStore(fname));
        auto filename = GetFilename(fname);
        return filename.size() >= sizeof(kCompressedStoreSuffix) &&
               std::equal(filename.cend() - (sizeof(kCompressedStoreSuffix) - 1), filename.cend(),
                          kCompressedStoreSuffix);
    }

    static constexpr char kPlainStoreSuffix[] = ".plain";

    bool IsPlainStore(const std::string & fname) {
        assert(IsStore(fname));
        auto filename = GetFilename(fname);
        return filename.size() >= sizeof(kPlainStoreSuffix) &&
               std::equal(filename.cend() - (sizeof(kPlainStoreSuffix) - 1), filename.cend(),
                          kPlainStoreSuffix);
    }

    static constexpr char kIndexPrefix[] = "index_";

    bool IsIndex(const std::string & fname) {
        auto filename = GetFilename(fname);
        return filename.size() >= sizeof(kIndexPrefix) &&
               std::equal(filename.cbegin(), filename.cbegin() + (sizeof(kIndexPrefix) - 1),
                          kIndexPrefix);
    }

    static constexpr char kStorePrefix[] = "store_";

    bool IsStore(const std::string & fname) {
        auto filename = GetFilename(fname);
        return filename.size() >= sizeof(kStorePrefix) &&
               std::equal(filename.cbegin(), filename.cbegin() + (sizeof(kStorePrefix) - 1),
                          kStorePrefix);
    }

    size_t GetStoreSeq(const std::string & fname) {
        assert(IsStore(fname));
        auto filename = GetFilename(fname);
        auto pos = filename.find('_') + 1;
        auto * begin = filename.data() + pos;
        auto * end = const_cast<char *>(filename.data()) + filename.size();
        return strtoull(begin, &end, 10);
    }

    size_t GetStoreLv(const std::string & fname) {
        assert(IsStore(fname));
        auto filename = GetFilename(fname);
        auto pos = filename.rfind('_') + 1;
        auto * begin = filename.data() + pos;
        auto * end = const_cast<char *>(filename.data()) + filename.size();
        return strtoull(begin, &end, 10);
    }

    void IndexFilename(size_t nth, const std::string & dirname,
                       std::string * fname) {
        char buf[128];
        int n = snprintf(buf, sizeof(buf), "index_%zu", nth);
        fname->assign(dirname);
        fname->append(buf, static_cast<size_t>(n));
        assert(IsIndex(*fname));
    }

    void StoreFilename(size_t seq, size_t lv, bool compress, const std::string & dirname,
                       std::string * fname) {
        char buf[128];
        int n = snprintf(buf, sizeof(buf), "store_%zu_%zu%s", seq, lv,
                         compress ? kCompressedStoreSuffix : kPlainStoreSuffix);
        fname->assign(dirname);
        fname->append(buf, static_cast<size_t>(n));
        assert(IsCompressedStore(*fname) || IsPlainStore(*fname));
    }
}