#include <iostream>
#include <map>
#include <thread>

#include "env.h"

#include "../include/db.h"

namespace levidb::db_test {
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

    class TextProvider {
    private:
        std::string k_;
        std::string v_;
        size_t cnt_ = 0;

    public:
        std::pair<Slice, Slice>
        ReadItem() {
            k_.clear();
            v_.clear();
            size_t h = std::hash<size_t>()(cnt_++);
            k_.append(reinterpret_cast<char *>(&h), sizeof(h));
            k_.append(std::to_string(h));
            v_.append(std::to_string(~h));
            return {{k_.c_str(), k_.size() + 1}, v_};
        };

        void SkipItem() {
            ++cnt_;
        }
    };

    void Run() {
        constexpr char kPathDB[] = "/tmp/levi-db";
        constexpr unsigned int kTestTimes = 10000;
        constexpr unsigned int kThreadNum = 4;

        auto * env = penv::Env::Default();
        if (env->FileExists(kPathDB)) {
            env->DeleteAll(kPathDB);
        }

        {
            ManifestorImpl manifestor;
            auto db = DB::Open(kPathDB, OpenOptions{&manifestor});
            {
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        TextProvider provider;
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, v] = provider.ReadItem();
                                db->Add(k, v);
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
            }
            {
                std::vector<std::thread> jobs;
                for (size_t i = 0; i < kThreadNum; ++i) {
                    jobs.emplace_back([&](size_t nth) {
                        std::string buf;
                        TextProvider provider;
                        for (size_t j = 0; j < kTestTimes; ++j) {
                            if (j % kThreadNum == nth) {
                                auto[k, v] = provider.ReadItem();
                                db->Get(k, &buf);
                                assert(v == buf);
                            } else {
                                provider.SkipItem();
                            }
                        }
                    }, i);
                }
                for (auto & job:jobs) {
                    job.join();
                }
            }
            {
                size_t num = 0;
                auto iter = db->GetIterator();
                for (iter->SeekToFirst();
                     iter->Valid();
                     iter->Next()) {
                    ++num;
                }
                assert(num == kTestTimes);
            }
        }
        std::cout << __PRETTY_FUNCTION__ << " - OK" << std::endl;
    }
}