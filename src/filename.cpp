#include "filename.h"

namespace levidb {
    static constexpr char kCompressedStoreSuffix[] = ".cprs";

    bool IsCompressedStore(const std::string & fname) {
        assert(IsStore(fname));
        return fname.size() >= sizeof(kCompressedStoreSuffix) &&
               std::equal(fname.cend() - (sizeof(kCompressedStoreSuffix) - 1), fname.cend(), kCompressedStoreSuffix);
    }

    static constexpr char kPlainStoreSuffix[] = ".plain";

    bool IsPlainStore(const std::string & fname) {
        assert(IsStore(fname));
        return fname.size() >= sizeof(kPlainStoreSuffix) &&
               std::equal(fname.cend() - (sizeof(kPlainStoreSuffix) - 1), fname.cend(), kPlainStoreSuffix);
    }

    static constexpr char kIndexPrefix[] = "index_";

    bool IsIndex(const std::string & fname) {
        return fname.size() >= sizeof(kIndexPrefix) &&
               std::equal(fname.cbegin(), fname.cbegin() + (sizeof(kIndexPrefix) - 1), kIndexPrefix);
    }

    static constexpr char kStorePrefix[] = "store_";

    bool IsStore(const std::string & fname) {
        return fname.size() >= sizeof(kStorePrefix) &&
               std::equal(fname.cbegin(), fname.cbegin() + (sizeof(kStorePrefix) - 1), kStorePrefix);
    }

    size_t GetStoreSeq(const std::string & fname) {
        assert(IsStore(fname));
        auto pos = fname.find('_') + 1;
        return std::stoull(fname, &pos);
    }

    size_t GetStoreLv(const std::string & fname) {
        assert(IsStore(fname));
        auto pos = fname.rfind('_') + 1;
        return std::stoull(fname, &pos);
    }

    void IndexFilename(size_t nth, std::string * fname) {
        char buf[128];
        int n = snprintf(buf, sizeof(buf), "index_%zu", nth);
        fname->assign(buf, static_cast<size_t>(n));
    }

    void StoreFilename(size_t seq, size_t lv, bool compress, std::string * fname) {
        char buf[128];
        int n = snprintf(buf, sizeof(buf), "store_%zu_%zu%s", seq, lv,
                         compress ? kCompressedStoreSuffix : kPlainStoreSuffix);
        fname->assign(buf, static_cast<size_t>(n));
    }
}