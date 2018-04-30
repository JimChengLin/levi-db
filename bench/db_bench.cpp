#include <map>

#include "../include/db.h"

namespace levidb::db_bench {
    class ManifestorImpl : public Manifestor {
    private:
        std::map<std::string, std::string, SliceComparator> map_;

    public:
        void Set(const Slice & k, const Slice & v) override {
            map_.emplace(k.ToString(), v.ToString());
        }

        bool Get(const Slice & k, std::string * v) const override {
            auto it = map_.find(k);
            if (it != map_.cend()) {
                v->assign(it->second);
                return true;
            }
            return false;
        }
    };

    void Run() {
        ManifestorImpl manifestor;
        auto db = DB::Open("/tmp/levi-db", OpenOptions{&manifestor});
    }
}